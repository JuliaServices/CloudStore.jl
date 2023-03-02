

# https://docs.aws.amazon.com/general/latest/gr/rande.html
const AWS_REGIONS = Set{String}([
    "us-east-2",
    "us-east-1",
    "us-west-1",
    "us-west-2",
    "af-south-1",
    "ap-east-1",
    "ap-south-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-south-1",
    "ap-northeast-3",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "ca-central-1",
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "eu-south-1",
    "eu-west-3",
    "eu-south-2",
    "eu-north-1",
    "eu-central-2",
    "me-south-1",
    "me-central-1",
    "sa-east-1",
    "us-gov-east-1",
    "us-gov-west-1",
])

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
function validate_bucket_name(bucket_name, accelerate)
    !(3 <= ncodeunits(bucket_name) <= 63)    && throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Bucket names must be between 3 (min) and 63 (max) characters long."))
    occursin("..", bucket_name)              && throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Bucket names must not contain two adjacent periods."))
    startswith(bucket_name, "xn--")          && throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Bucket names must not start with the prefix `xn--`."))
    endswith(bucket_name, "-s3alias")        && throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Bucket names must not end with the suffix `-s3alias`."))
    accelerate && occursin(".", bucket_name) && throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Buckets used with Amazon S3 Transfer Acceleration can't have dots (.) in their names."))
    !contains(bucket_name, r"^[a-z0-9][\.\-a-z0-9]+[a-z0-9]$") &&
        throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-). Bucket names must begin and end with a letter or number."))
    count(==('.'), bucket_name) == 3 && all(!isnothing, tryparse(Int, s) for s in split(bucket_name, '.')) &&
        throw(ArgumentError("Validation failed for `bucket` name $(repr(bucket_name)): Bucket names must not be formatted as an IP address (for example, 192.168.5.4)"))
    return bucket_name
end

# https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
function validate_container_name(container_name)
    !(3 <= ncodeunits(container_name) <= 63) && throw(ArgumentError("Validation failed for `container` name $(repr(container_name)): Container names must be between 3 (min) and 63 (max) characters long."))
    occursin("--", container_name)           && throw(ArgumentError("Validation failed for `container` name $(repr(container_name)): Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive dashes are not permitted in container names."))
    !contains(container_name, r"^[a-z0-9][\-a-z0-9]+[a-z0-9]$") &&
        throw(ArgumentError("Validation failed for `container` name $(repr(container_name)): Container names can consist only of lowercase letters, numbers and, and hyphens (-). Bucket names must begin and end with a letter or number."))
    return container_name
end

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
function validate_key(key)
    ncodeunits(key) > 1024 && throw(ArgumentError("Validation failed for `key` $(repr(key)): The key name must be shorter than 1025 bytes."))
    return key
end

function validate_region(region)
    region in AWS_REGIONS || throw(ArgumentError("Validation failed for `region` $(repr(region))"))
    return region
end

# https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
function validate_blob(blob)
    !(1 < ncodeunits(blob) <= 1024) && throw(ArgumentError("Validation failed for `blob` $(repr(blob)): A blob name must be at least one character long and cannot be more than 1,024 characters long, for blobs in Azure Storage."))
    count(==('/'), blob) > 253      && throw(ArgumentError("Validation failed for `blob` $(repr(blob)): The number of path segments comprising the blob name cannot exceed 254. A path segment is the string between consecutive delimiter characters (e.g., the forward slash '/') that corresponds to the name of a virtual directory."))
    return blob
end

# https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview
function validate_account_name(account)
    !(3 <= ncodeunits(account) <= 24)  && throw(ArgumentError("Validation failed for `account` name $(repr(account)): Storage account names must be between 3 and 24 characters in length."))
    !contains(account, r"^[a-z0-9]+$") && throw(ArgumentError("Validation failed for `account` name $(repr(account)): Storage account names  may contain numbers and lowercase letters only."))
    return account
end

function _validate_azure(ok::Bool, host, account, container, blob)
    return (
        ok,
        isnothing(host) ? nothing : replace(String(host), "azure" => "https"; count=1),
        String(validate_account_name(account)),
        isnothing(container) ? "" : String(validate_container_name(container)),
        isnothing(blob) ? "" : String(validate_blob(blob)),
    )
end

function _validate_aws(ok::Bool, accelerate, host, bucket, region, key)
    return (
        ok,
        accelerate,
        isnothing(host) ? nothing : replace(String(host), "s3" => "http"; count=1),
        String(validate_bucket_name(bucket, accelerate)),
        isnothing(region) || isempty(region) ? "" : String(validate_region(region)),
        isnothing(key) ? "" : String(validate_key(key)),
    )
end

# try to parse cloud-specific url schemes and dispatch
function parseAzureAccountContainerBlob(url; parseLocal::Bool=false)
    url = String(url)
    # https://myaccount.blob.core.windows.net/mycontainer/myblob
    # https://myaccount.blob.core.windows.net/mycontainer
    m = match(r"^(https|azure)://(?<account>[^\.]+?)(\.blob\.core\.windows\.net)?/(?<container>[^/]+?)(?:/(?<blob>.+))?$", url)
    m !== nothing && return _validate_azure(true, nothing, m[:account], m[:container], m[:blob])
    if parseLocal
        # "https://127.0.0.1:45942/devstoreaccount1/jl-azurite-21807/"
        m = match(r"^(?<host>(https|azure)://[\d|\.|:]+?)/(?<account>[^/]+?)/(?<container>[^/]+?)(?:/(?<blob>.+))?$", url)
        m !== nothing && return _validate_azure(true, m[:host], m[:account], m[:container], m[:blob])
    end
    # azure://myaccount/mycontainer/myblob
    # azure://myaccount/mycontainer
    # azure://myaccount
    m = match(r"^azure://(?<account>[^/]+)(?:/(?<container>.+))?(?:/(?<blob>.+))?$"i, url)
    m !== nothing && return return _validate_azure(true, nothing, m[:account], m[:container], m[:blob])
    return (false, nothing, "", "", "")
end

function parseAWSBucketRegionKey(url; parseLocal::Bool=false)
    url = String(url)
    # https://bucket-name.s3-accelerate.region-code.amazonaws.com/key-name
    # https://bucket-name.s3-accelerate.region-code.amazonaws.com
    # https://bucket-name.s3-accelerate.amazonaws.com/key-name
    # https://bucket-name.s3-accelerate.amazonaws.com
    # https://bucket-name.s3.region-code.amazonaws.com/key-name
    # https://bucket-name.s3.region-code.amazonaws.com
    # https://bucket-name.s3.amazonaws.com/key-name
    # https://bucket-name.s3.amazonaws.com
    m = match(r"^https://(?<bucket>[^\.]+)\.s3(?<accelerate>-accelerate)?(?:\.(?<region>[^\.]+))?\.amazonaws\.com(?:/(?<key>.+))?$", url)
    m !== nothing && return _validate_aws(true, !isnothing(m[:accelerate]), nothing, m[:bucket], m[:region], m[:key])
    # https://s3.region-code.amazonaws.com/bucket-name/key-name
    # https://s3.region-code.amazonaws.com/bucket-name
    m = match(r"^https://s3(?:\.(?<region>[^\.]+))?\.amazonaws\.com/(?<bucket>[^/]+)(?:/(?<key>.+))?$", url)
    m !== nothing && return _validate_aws(true, false, nothing, m[:bucket], m[:region], m[:key])
    if parseLocal
        # "http://127.0.0.1:27181/jl-minio-4483/"
        m = match(r"^(?<host>(http|s3)://[\d|\.|:]+?)/(?<bucket>[^/]+?)(?:/(?<key>.+))?$", url)
        m !== nothing && return _validate_aws(true, false, m[:host], m[:bucket], "", m[:key])
    end
    # S3://bucket-name/key-name
    # S3://bucket-name
    m = match(r"^s3://(?<bucket>[^/]+)(?:/(?<key>.+))?$"i, url)
    m !== nothing && return _validate_aws(true, false, nothing, m[:bucket], "", m[:key])
    return (false, false, nothing, "", "", "")
end

function parseGCPBucketObject(url)
    url = String(url)
    # https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
    # https://storage.googleapis.com/BUCKET_NAME
    m = match(r"^https://storage\.googleapis\.com/(?<bucket>[^/]+)(?:/(?<key>.+))?$", url)
    m !== nothing && return (true, String(m[:bucket]), String(something(m[:object], "")))
    # https://BUCKET_NAME.storage.googleapis.com/OBJECT_NAME
    # https://BUCKET_NAME.storage.googleapis.com
    m = match(r"^https://(?<bucket>[^\.]+)\.storage\.googleapis\.com(?:/(?<key>.+))?$", url)
    m !== nothing && return (true, String(m[:bucket]), String(something(m[:object], "")))
    # https://storage.googleapis.com/download/storage/v1/b/BUCKET_NAME/o/OBJECT_NAME?alt=media
    # https://storage.googleapis.com/download/storage/v1/b/BUCKET_NAME
    m = match(r"^https://storage\.googleapis\.com/download/storage/v1/b/(?<bucket>[^/]+)(?:/o/(?<key>.+?))?(\?alt=media)?$", url)
    m !== nothing && return (true, String(m[:bucket]), String(something(m[:object], "")))
    return (false, "", "")
end

function parseURLForDispatch(url, region, nowarn)
    # try to parse cloud-specific url schemes and dispatch
    ok, accelerate, host, bucket, reg, key = parseAWSBucketRegionKey(url)
    region = isempty(reg) ? region : reg
    if ok && region === nothing
        nowarn || @warn "`region` keyword argument not provided to `CloudStore.get` and undetected from url.  Defaulting to `us-east-1`"
        region = AWS.AWS_DEFAULT_REGION
    end
    ok && return (AWS.Bucket(bucket, region; accelerate, host), key)
    ok, host, account, container, blob = parseAzureAccountContainerBlob(url)
    ok && return (Azure.Container(container, account; host), blob)
    # ok, bucket, object = parseGCPBucketObject(url)
    # ok && return (GCP.Bucket(bucket), object)
    error("couldn't determine cloud from string url: `$url`")
end
