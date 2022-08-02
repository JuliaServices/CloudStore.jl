"""
CloudBase provides very simple HTTP clients for cloud services
(AWS, Azure, Google Cloud), which consists of 2 main components:
  * Computing credentials necessary for authenticated cloud requests
  * Signing requests with the credentials

CloudStore provides a very simple, consistent, and performant API for
object management with cloud services.
  * Listing objects
  * Getting objects, using concurrent ranged streaming downloads for large files
  * Putting objects, using concurrent multipart streaming uploads for large files

* allow compressing objects auto when putting; decompressing auto when getting
* allow getting object direct to memory, or streaming out to file
* allow public get from s3:// azure:// urls
"""
module CloudStore

using CloudBase



# GCP

# credentials
 # https://cloud.google.com/storage/docs/authentication#user_accounts


# AWS

# credentials
 # https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence

# GetObject: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
# ListObjects: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
# PutObject: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html


end # module CloudStore
