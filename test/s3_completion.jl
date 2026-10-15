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
