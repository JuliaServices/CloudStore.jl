using Base64
import XMLDict

function manual_server(f, handler)
    requests = Channel{Any}(128)
    server = HTTP.serve!(0; listenany=true, verbose=false) do request
        put!(requests, (method=request.method, target=request.target,
            headers=collect(request.headers), body=manual_bytes(request.body)))
        handler(request)
    end
    try
        host = "http://127.0.0.1:$(HTTP.port(server))"
        f(S3.Bucket("manual-bucket", "us-east-1"; host),
            Blobs.Container("manual-container", "manualaccount"; host), requests)
    finally
        close(server)
        close(requests)
    end
end

manual_bytes(body::AbstractVector) = copy(body)
manual_bytes(body) = collect(codeunits(String(body)))
function manual_take(requests)
    isready(requests) || error("request fixture did not record the completed request")
    return take!(requests)
end

manual_query(request) = HTTP.queryparams(HTTP.URI(request.target))
function manual_header(request, name)
    for (key, value) in request.headers
        lowercase(key) == lowercase(name) && return value
    end
    return ""
end
manual_xml(tag, value) = XMLDict.node_xml(tag, value)

@testset "Manual multipart requests" begin
    @testset "S3 receipts, pages, completion and ownership" begin
        id = "opaque+/=&雪"
        headers = ["x-amz-meta-source" => "unchanged"]
        data = UInt8[0, 1, 2, 3, 4]
        completion = Ref(:success)
        handler = function(request)
            query = manual_query(request)
            if request.method == "DELETE"
                return HTTP.Response(204)
            elseif haskey(query, "uploads")
                return HTTP.Response(200, [], manual_xml("InitiateMultipartUploadResult", Dict("UploadId" => id)))
            elseif request.method == "PUT"
                return HTTP.Response(200, ["ETag" => "\"part<&\"", "x-amz-checksum-sha256" => "digest=="])
            elseif request.method == "GET"
                firstpage = query["part-number-marker"] == "0"
                part = Dict("PartNumber" => firstpage ? "2" : "5", "ETag" => "\"part<&\"",
                    "Size" => "3", "LastModified" => "2026-01-01T00:00:00.000Z", "ChecksumSHA256" => "digest==")
                page = Dict("UploadId" => id, "IsTruncated" => firstpage ? "true" : "false", "Part" => part)
                firstpage && (page["NextPartNumberMarker"] = "2")
                return HTTP.Response(200, [], manual_xml("ListPartsResult", page))
            elseif completion[] == :error
                return HTTP.Response(200, ["ETag" => "\"misleading\""], " \n" * manual_xml("Error", Dict("Code" => "InvalidPart", "Message" => "missing part")))
            elseif completion[] == :retryable
                return HTTP.Response(503, [], "busy")
            else
                return HTTP.Response(200, [], manual_xml("CompleteMultipartUploadResult", Dict("ETag" => "\"finished\"")))
            end
        end
        manual_server(handler) do bucket, _, requests
            key = "literal +%/雪&=.bin"
            @test S3.createmultipartupload(bucket, key; headers, contentType="application/example") == id
            created = manual_take(requests)
            @test manual_query(created) == Dict("uploads" => "")
            @test HTTP.unescapeuri(HTTP.URI(created.target).path) == "/manual-bucket/" * key
            @test manual_header(created, "Content-Type") == "application/example"
            receipt = S3.uploadpart(bucket, key, id, 2, @view(data[2:4]); headers)
            uploaded = manual_take(requests)
            @test uploaded.body == UInt8[1, 2, 3]
            @test manual_query(uploaded) == Dict("uploadId" => id, "partNumber" => "2")
            @test receipt == (number=2, etag="\"part<&\"", size=Int64(3), checksums=Dict("ChecksumSHA256" => "digest=="))
            @test fieldnames(typeof(receipt)) == (:number, :etag, :size, :checksums)
            S3.uploadpart(bucket, key, id, 1, @view(data[1:2:5]); headers)
            @test manual_take(requests).body == UInt8[0, 2, 4]
            parts = S3.listparts(bucket, key, id; maxparts=1, headers)
            @test [part.number for part in parts] == [2, 5]
            @test [part.size for part in parts] == [3, 3]
            @test parts[1].etag == receipt.etag
            @test parts[1].checksums == receipt.checksums
            @test parts[1].last_modified == "2026-01-01T00:00:00.000Z"
            @test manual_query(manual_take(requests))["part-number-marker"] == "0"
            @test manual_query(manual_take(requests))["part-number-marker"] == "2"
            receipts = [receipt, merge(receipt, (number=5,))]
            conditional = ["If-None-Match" => "*"]
            @test S3.completemultipartupload(bucket, key, id, receipts; headers=conditional) == "finished"
            completed = manual_take(requests)
            nodes = XMLDict.xml_dict(String(completed.body))["CompleteMultipartUpload"]["Part"]
            @test [node["PartNumber"] for node in nodes] == ["2", "5"]
            @test all(node -> node["ETag"] == receipt.etag && node["ChecksumSHA256"] == "digest==", nodes)
            @test manual_header(completed, "If-None-Match") == "*"
            @test conditional == ["If-None-Match" => "*"]
            for invalid in (reverse(receipts), [receipt, receipt], [], [(number=0, etag="x")],
                [(number=1, etag="")], [(number=1, etag="x", checksums=Dict("ChecksumUnknown" => "value"))])
                @test_throws ArgumentError S3.completemultipartupload(bucket, key, id, invalid)
            end
            for invalid in (0, 10001)
                @test_throws ArgumentError S3.uploadpart(bucket, key, id, invalid, data)
                @test_throws ArgumentError S3.listparts(bucket, key, id; maxparts=invalid)
            end
            @test_throws ArgumentError S3.listparts(bucket, key, "")
            @test_throws ArgumentError S3.uploadpart(bucket, key, id, 1, data; query=Dict())
            @test_throws ArgumentError S3.completemultipartupload(bucket, key, id, receipts; retry=true)
            @test_throws ArgumentError S3.createmultipartupload(bucket, key; redirect=true)
            @test !isready(requests)
            completion[] = :error
            @test_throws ErrorException("S3 CompleteMultipartUpload failed (InvalidPart): missing part") S3.completemultipartupload(bucket, key, id, receipts)
            @test manual_take(requests).method == "POST"
            @test !isready(requests) # Manual failures must not abort.
            completion[] = :retryable
            @test_throws Exception S3.completemultipartupload(bucket, key, id, receipts)
            @test manual_take(requests).method == "POST"
            @test !isready(requests) # A final request is not automatically retried.
            @test S3.abortmultipartupload(bucket, key, id; headers) === nothing
            @test manual_take(requests).method == "DELETE"
            @test data == UInt8[0, 1, 2, 3, 4]
            @test headers == ["x-amz-meta-source" => "unchanged"]
        end
    end

    @testset "S3 malformed pages and optional checksum metadata" begin
        for page in (
            Dict("IsTruncated" => "true", "NextPartNumberMarker" => "0"),
            Dict("IsTruncated" => "true", "NextPartNumberMarker" => "10001"),
            Dict("IsTruncated" => "maybe"),
            Dict("IsTruncated" => "false", "UploadId" => "wrong"),
            Dict("IsTruncated" => "false", "Part" => Dict("PartNumber" => "1", "ETag" => "x", "Size" => "-1", "LastModified" => "time")),
            Dict("IsTruncated" => "false", "Part" => [Dict("PartNumber" => "1", "ETag" => "x", "Size" => "1", "LastModified" => "time") for _ in 1:2]),
        )
            manual_server(_ -> HTTP.Response(200, [], manual_xml("ListPartsResult", page))) do bucket, _, requests
                @test_throws Exception S3.listparts(bucket, "key", "id")
                @test manual_take(requests).method == "GET"
                @test !isready(requests)
            end
        end
        manual_server(_ -> HTTP.Response(200, [], "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>")) do bucket, _, _
            @test isempty(S3.listparts(bucket, "key", "id"))
        end
        for repeated in (false, true)
            handler = function(request)
                marker = manual_query(request)["part-number-marker"]
                body = marker == "0" || repeated ?
                    "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>2</NextPartNumberMarker></ListPartsResult>" :
                    "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"
                return HTTP.Response(200, [], body)
            end
            manual_server(handler) do bucket, _, requests
                if repeated
                    @test_throws ErrorException("S3 ListParts returned a non-advancing marker") S3.listparts(bucket, "key", "id")
                else
                    @test isempty(S3.listparts(bucket, "key", "id"))
                end
                @test manual_query(manual_take(requests))["part-number-marker"] == "0"
                @test manual_query(manual_take(requests))["part-number-marker"] == "2"
                @test !isready(requests)
            end
        end
        for header in (["ETag" => "\"part\"", "x-amz-checksum-future" => "new"], ["x-amz-checksum-sha256" => "digest"])
            manual_server(_ -> HTTP.Response(200, header)) do bucket, _, _
                @test_throws Exception S3.uploadpart(bucket, "key", "id", 1, UInt8[1])
            end
        end
        checksums = Dict(name => "opaque-$name==" for name in (
            "ChecksumCRC32", "ChecksumCRC32C", "ChecksumCRC64NVME", "ChecksumMD5",
            "ChecksumSHA1", "ChecksumSHA256", "ChecksumSHA512", "ChecksumXXHASH128",
            "ChecksumXXHASH3", "ChecksumXXHASH64"))
        handler = function(request)
            if request.method == "PUT"
                headers = ["x-amz-checksum-" * lowercase(name[9:end]) => value for (name, value) in checksums]
                push!(headers, "ETag" => "\"part\"")
                return HTTP.Response(200, headers)
            elseif request.method == "GET"
                part = merge(checksums, Dict("PartNumber" => "1", "ETag" => "\"part\"", "Size" => "1", "LastModified" => "time"))
                return HTTP.Response(200, [], manual_xml("ListPartsResult", Dict("IsTruncated" => "false", "Part" => part)))
            end
            return HTTP.Response(200, [], "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>")
        end
        manual_server(handler) do bucket, _, requests
            receipt = S3.uploadpart(bucket, "key", "id", 1, UInt8[1])
            @test receipt.checksums == checksums
            manual_take(requests)
            @test only(S3.listparts(bucket, "key", "id")).checksums == checksums
            manual_take(requests)
            @test S3.completemultipartupload(bucket, "key", "id", [receipt]) == "done"
            node = XMLDict.xml_dict(String(manual_take(requests).body))["CompleteMultipartUpload"]["Part"]
            @test all(node[name] == value for (name, value) in checksums)
        end
    end

    @testset "Azure explicit block identity and selector order" begin
        a, b = base64encode("upload-01"), base64encode("upload-02")
        headers = ["x-ms-meta-source" => "unchanged"]
        data = UInt8[0, 1, 2, 3, 4]
        outcome = Ref(:success)
        handler = function(request)
            if request.method == "GET"
                body = "<BlockList><CommittedBlocks><Block><Name>$a</Name><Size>1</Size></Block><Block><Name>$a</Name><Size>1</Size></Block></CommittedBlocks><UncommittedBlocks><Block><Name>$a</Name><Size>3</Size></Block><Block><Name>$b</Name><Size>2</Size></Block></UncommittedBlocks></BlockList>"
                return HTTP.Response(200, ["ETag" => "\"previous\""], body)
            elseif outcome[] == :retryable
                return HTTP.Response(503, [], "busy")
            end
            return HTTP.Response(201, ["ETag" => "\"published\""])
        end
        manual_server(handler) do _, container, requests
            receipt = Blobs.stageblock(container, "literal +%/雪", a, @view(data[2:4]); headers)
            @test receipt == (id=a, size=Int64(3))
            staged = manual_take(requests)
            @test staged.body == UInt8[1, 2, 3]
            @test manual_query(staged) == Dict("comp" => "block", "blockid" => a)
            Blobs.stageblock(container, "key", a, @view(data[1:2:5]); headers)
            @test manual_take(requests).body == UInt8[0, 2, 4]
            inventory = Blobs.listblocks(container, "key"; headers)
            @test inventory.committed == [(id=a, size=Int64(1)), (id=a, size=Int64(1))]
            @test inventory.uncommitted == [(id=a, size=Int64(3)), (id=b, size=Int64(2))]
            @test inventory.etag == "\"previous\""
            @test manual_query(manual_take(requests))["blocklisttype"] == "all"
            blocks = [(id=b, state=:uncommitted), (id=a, state=:committed), (id=b, state=:uncommitted)]
            @test Blobs.commitblocks(container, "key", blocks; headers, contentType="application/example") == "published"
            committed = manual_take(requests)
            @test String(committed.body) == "<BlockList><Uncommitted>$b</Uncommitted><Committed>$a</Committed><Uncommitted>$b</Uncommitted></BlockList>"
            @test manual_header(committed, "x-ms-blob-content-type") == "application/example"
            @test Blobs.commitblocks(container, "key", []) == "published"
            @test String(manual_take(requests).body) == "<BlockList></BlockList>"
            for id in ("", "not base64", base64encode(zeros(UInt8, 65)), "YQ")
                @test_throws Exception Blobs.stageblock(container, "key", id, data)
            end
            @test_throws ArgumentError Blobs.commitblocks(container, "key", [(id=a, state=:committed), (id=a, state=:uncommitted)])
            @test_throws ArgumentError Blobs.commitblocks(container, "key", [(id=a, state=:missing)])
            @test_throws ArgumentError Blobs.commitblocks(container, "key", [(id=a, state=:latest), (id=base64encode("x"), state=:latest)])
            @test_throws ArgumentError Blobs.listblocks(container, "key"; state=:missing)
            @test_throws ArgumentError Blobs.stageblock(container, "key", a, data; query=Dict())
            @test_throws ArgumentError Blobs.commitblocks(container, "key", blocks; retry=true)
            @test !isready(requests)
            outcome[] = :retryable
            @test_throws Exception Blobs.commitblocks(container, "key", blocks)
            @test manual_take(requests).method == "PUT"
            @test !isready(requests)
            @test data == UInt8[0, 1, 2, 3, 4]
            @test headers == ["x-ms-meta-source" => "unchanged"]
        end
        for body in ("<BlockList/>", "<BlockList><CommittedBlocks/><UncommittedBlocks/></BlockList>")
            manual_server(_ -> HTTP.Response(200, [], body)) do _, container, _
                inventory = Blobs.listblocks(container, "key")
                @test isempty(inventory.committed)
                @test isempty(inventory.uncommitted)
                @test inventory.etag === nothing
            end
        end
    end
end
