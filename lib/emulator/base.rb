
module OssEmulator

  module Version
    VERSION = '1.0.0'
    VERSION_STRING = 'Emulator V1.0.0'
  end # Version

  module Store
    STORE_ROOT_DIR = '../store'
    
    BUCKET_METADATA = '.metadata_bucket_oss_aliyun_ALIBABA'

    OBJECT_METADATA = '.metadata_object_oss_aliyun_ALIBABA'
    OBJECT_CONTENT_PREFIX = '.content_object_oss_aliyun_ALIBABA-'
    OBJECT_CONTENT = '.content_object_oss_aliyun_ALIBABA-1'
    OBJECT_CONTENT_TWO = '.content_object_oss_aliyun_ALIBABA-2'
  end # Store

  module Bucket
    MAX_BUCKET_NUM = 30
    BUCKET_NAME_CHAR_SET = '1234567890abcdefghijklmnopqrstuvwxyz-'
    MAX_BUCKET_NAME_LENGTH = 63
  end # Bucket

  module Object
    MAX_OBJECT_NAME_LENGTH = 1023
    MAX_OBJECT_NAME_SLICE_LENGTH = 255
    MAX_OBJECT_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5G = 5*1024*1024*1024 Bytes
    STREAM_CHUNK_SIZE = 32 * 1024
  end # Object

  module HttpMsg
    HOST = 'localhost'
    HOSTNAMES = ['localhost', 'oss.aliyun.com', 'oss.localhost']

    ALIYUN_OSS_SERVER = 'AliyunOSS'
    REQUEST_ID = '1234567890ABCDEF12345678'

    SUBSECOND_PRECISION = 3

    XMLNS = 'http://doc.oss-cn-hangzhou.aliyuncs.com'
    OWNER_ID = '00220120222'
    OWNER_DISPLAY_NAME = '1390402650033798'
  end # HttpMsg
  
  module ACL
    PUBLIC_READ_WRITE = 'public-read-write'
    PUBLIC_READ = 'public-read'
    PRIVATE = 'private'
    DEFAULT = 'default'

    OSS_ACL = [ PUBLIC_READ_WRITE, PUBLIC_READ, PRIVATE, DEFAULT ]
    BUCKET_ACL_LIST = [ PUBLIC_READ_WRITE, PUBLIC_READ, PRIVATE ]
    OBJECT_ACL_LIST = [ PUBLIC_READ_WRITE, PUBLIC_READ, PRIVATE, DEFAULT ]
  end # ACL

  module ErrorCode
    NO_MODIFIED            = { error_code: 'NoModified', status_code: 304, message: 'The object has not been modified.' }
    BAD_REQUEST            = { error_code: 'BadRequest', status_code: 400, message: 'The server cannot understand the request.' }
    TOO_MANY_BUCKETS       = { error_code: 'TooManyBuckets', status_code: 400, message: 'Bucket number exceeds the limit.' }
    INVALID_BUCKET_NAME    = { error_code: 'InvalidBucketName', status_code: 400, message: 'The bucket name is invalid.' }
    INVALID_OBJECT_NAME    = { error_code: 'InvalidObjectName', status_code: 400, message: 'The object name is invalid.' }
    INVALID_ARGUMENT       = { error_code: 'InvalidArgument', status_code: 400, message: 'The file size should be less than 5G.' }
    FILE_PART_NO_EXIST     = { error_code: 'FilePartNotExist', status_code: 400, message: 'The file part does not exist.' }
    ACCESS_DENIED          = { error_code: 'AccessDenied', status_code: 403, message: 'The access is forbidden.' }
    NO_SUCH_BUCKET         = { error_code: 'NoSuchBucket', status_code: 404, message: 'The bucket does not exist.' }
    NO_SUCH_KEY            = { error_code: 'NoSuchKey', status_code: 404, message: 'The specified object does not exist.' }
    NOT_FOUND              = { error_code: 'NotFound', status_code: 404, message: 'The file has not been found.' }
    BUCKET_ALREADY_EXISTS  = { error_code: 'BucketAlreadyExists', status_code: 409, message: 'The bucket already exists.' }
    BUCKET_NOT_EMPTY       = { error_code: 'BucketNotEmpty', status_code: 409, message: 'The bucket is not empty.' }
    OBJECT_NOT_APPENDABLE  = { error_code: 'ObjectNotAppendable', status_code: 409, message: 'The object is not appendable.' }
    MISSING_CONTENT_LENGTH = { error_code: 'MissingContentLength', status_code: 411, message: 'No Content-Length in request header.' }
    INTERNAL_ERROR         = { error_code: 'InternalError', status_code:500, message: 'An internal error occurs inside OSS.' }
    NOT_IMPLEMENTED        = { error_code: 'NotImplemented', status_code:501, message: 'The function is not supported yet.' }
  end # ErrorCode

end # OssEmulator