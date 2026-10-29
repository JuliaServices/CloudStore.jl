@testset "S3 multipart completion responses" begin
    # :retry and :status fail the first completion attempt (InternalError in an
    # HTTP 200 body, or HTTP 500); :error returns InternalError every time.
    for outcome in (:success, :retry, :status, :error), upload in (:put, :stream)
        @testset "$outcome through $upload" begin
            requests = Channel{HTTP.Request}(8)
            completions = Ref(0)
            server = HTTP.serve!(0; listenany=true, verbose=false) do request
                put!(requests, request)
                if request.method == "DELETE"
                    return HTTP.Response(204)
                elseif request.method == "PUT"
                    return HTTP.Response(200, ["ETag" => "\"part-etag\""])
                elseif occursin("uploads", request.target)
                    return HTTP.Response(200, [],
                        "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>")
                end
                completions[] += 1
                failed = outcome == :error || (outcome != :success && completions[] == 1)
                if failed && outcome == :status
                    return HTTP.Response(500, [], "<Error><Code>InternalError</Code></Error>")
                elseif !failed
                    return HTTP.Response(200, [], " \n\t" * """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Location>http://localhost/bucket-name/key</Location>
                            <Bucket>bucket-name</Bucket><Key>key</Key><ETag>"object-etag"</ETag>
                        </CompleteMultipartUploadResult>
                        """)
                else
                    # S3 can send keepalive whitespace before its final XML response.
                    return HTTP.Response(200, ["ETag" => "\"misleading-etag\""], " \n\t" * """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <Error><Code>InternalError</Code><Message>Completion failed.</Message>
                            <RequestId>request-id</RequestId><HostId>host-id</HostId>
                        </Error>
                        """)
                end
            end
            try
                port = HTTP.port(server)
                bucket = S3.Bucket("bucket-name", "us-east-1"; host="http://127.0.0.1:$port")
                credentials = S3.Credentials("test-access-key", "test-secret-key")
                data = UInt8[1, 2, 3]
                headers = ["x-amz-meta-test" => "metadata"]
                stream = nothing
                result = try
                    if upload == :put
                        S3.put(bucket, "key", data; credentials, headers,
                            multipartThreshold=1, partSize=3, batchSize=1).eTag
                    else
                        stream = CloudStore.MultipartUploadStream(bucket, "key"; credentials, headers)
                        write(stream, data)
                        close(stream)
                    end
                catch exception
                    exception
                end
                if outcome != :error
                    @test result == "object-etag"
                    if stream !== nothing
                        @test stream.closed
                        @test !stream.aborted
                    end
                else
                    @test result isa ErrorException
                    @test occursin("InternalError", sprint(showerror, result))
                    @test occursin("Completion failed.", sprint(showerror, result))
                    if stream !== nothing
                        @test !stream.closed
                        @test stream.aborted
                        @test stream.exc === result
                    end
                end
                @test data == UInt8[1, 2, 3]
                @test headers == ["x-amz-meta-test" => "metadata"]
                close(requests)
                observed = collect(requests)
                @test [request.method for request in observed] ==
                    (outcome == :success ? ["POST", "PUT", "POST"] :
                     outcome == :error ? ["POST", "PUT", "POST", "POST", "POST", "DELETE"] :
                     ["POST", "PUT", "POST", "POST"])
                @test String(observed[2].body) == String(copy(data))
                @test occursin("uploadId=upload-id", observed[3].target)
                @test occursin("part-etag", String(observed[3].body))
            finally
                close(server)
            end
        end
    end
end

@testset "S3 completion after a lost response" begin
    # The first completion completes the upload but its response fails (HTTP 500, which
    # HTTP retries). The retry finds no upload (404), or, with a write condition, fails it
    # against the stored object (412). The upload ID is then gone, and the object's ETag
    # shows whether it is this upload: S3 derives it from the part ETags. A failed
    # condition leaves the upload open, so a 412 with the upload still listed is final.
    parttags = ("\"00112233445566778899aabbccddeeff\"", "\"ffeeddccbbaa99887766554433221100\"")
    expected = "2c9fc96c5e9c12cc2e04537710e74a63-2"
    cases = ((404, true, true), (404, false, true), (412, true, true), (412, false, true), (412, true, false))
    for (status, ours, gone) in cases, upload in (:put, :stream)
        methods = Channel{String}(16)
        conditions = Channel{String}(16)
        completions = Ref(0)
        server = HTTP.serve!(0; listenany=true, verbose=false) do request
            put!(methods, request.method)
            request.method == "DELETE" && return HTTP.Response(204)
            request.method == "HEAD" && return HTTP.Response(200, ["ETag" => ours ? "\"$expected\"" : "\"other\""])
            request.method == "GET" && return gone ?
                HTTP.Response(404, "<Error><Code>NoSuchUpload</Code></Error>") :
                HTTP.Response(200, "<ListPartsResult><UploadId>upload-id</UploadId><IsTruncated>false</IsTruncated></ListPartsResult>")
            if request.method == "PUT"
                number = parse(Int, HTTP.queryparams(HTTP.URI(request.target))["partNumber"])
                return HTTP.Response(200, ["ETag" => parttags[number]])
            end
            occursin("uploads", request.target) && return HTTP.Response(200,
                "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>")
            put!(conditions, HTTP.header(request, "If-None-Match", ""))
            completions[] += 1
            completions[] == 1 && return HTTP.Response(500, "<Error><Code>InternalError</Code></Error>")
            return HTTP.Response(status, status == 404 ?
                "<Error><Code>NoSuchUpload</Code></Error>" : "<Error><Code>PreconditionFailed</Code></Error>")
        end
        try
            bucket = S3.Bucket("bucket-name", "us-east-1"; host="http://127.0.0.1:$(HTTP.port(server))")
            credentials = S3.Credentials("test-access-key", "test-secret-key")
            headers = status == 412 ? ["If-None-Match" => "*"] : []
            logger = Test.TestLogger()
            result = Base.with_logger(logger) do
                try
                    if upload == :put
                        S3.put(bucket, "key", UInt8[1, 2, 3, 4, 5, 6]; credentials, headers,
                            multipartThreshold=1, partSize=3, batchSize=1).eTag
                    else
                        CloudStore.MultipartUploadStream(bucket, "key"; credentials, headers) do io
                            write(io, UInt8[1, 2, 3])
                            write(io, UInt8[4, 5, 6])
                        end
                    end
                catch exception
                    exception
                end
            end
            # Gone but not shown by the ETag: the error is kept and says the write may be stored.
            @test any(log -> occursin("may have stored", log.message), logger.logs) == (status == 412 && gone && !ours)
            close(methods)
            close(conditions)
            sent = collect(methods)
            @test collect(conditions) == (status == 412 ? ["*", "*"] : ["", ""])
            # NoSuchUpload already says the upload is gone; a 412 needs ListParts (GET).
            checks = status == 412 ? (gone ? ["GET", "HEAD"] : ["GET"]) : ["HEAD"]
            @test sent == ["POST", "PUT", "PUT", "POST", "POST", checks..., (ours && gone ? () : ("DELETE",))...]
            if ours && gone
                @test result == expected
            else
                @test result isa StatusError && result.status == status
            end
        finally
            close(server)
        end
    end
end
