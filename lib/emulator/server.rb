require 'time'
require 'webrick'
require 'webrick/https'
require 'openssl'
require 'cgi'
require 'ipaddr'
require 'emulator/config'
require 'emulator/request'
require 'emulator/response'
require 'emulator/bucket'
require 'emulator/object'
require 'emulator/multipart'

module OssEmulator
  class Servlet < WEBrick::HTTPServlet::AbstractServlet
    def initialize(server)
      super(server)
    end

    def do_HEAD(request, response)
      req = OssEmulator::Request.new(request)
      req.parse()

      case req.cmd
      when Request::HEAD_OBJECT 
        Object.head_object(req.bucket, req.object, response)
      else 
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
      end #case
    rescue
      OssResponse.response_error(response, ErrorCode::INTERNAL_ERROR)
    end # do_HEAD

    def do_GET(request, response)
      req = OssEmulator::Request.new(request)
      req.parse()

      case req.cmd
      when Request::LIST_BUCKETS # =GetService
        Bucket.list_buckets(response)
      when Request::GET_BUCKET  # =LIST_OBJECTS
        Bucket.get_bucket(req.bucket, req, response)
      when Request::GET_BUCKET_ACL
        Bucket.get_bucetk_acl(req.bucket, response)
      when Request::GET_BUCKET_INFO
        Bucket.get_bucetk_info(req.bucket, response)
      when Request::GET_BUCKET_LOCATION
        Bucket.get_bucket_location(response)
      when Request::GET_BUCKET_LOGGING
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::GET_BUCKET_REFERER
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::GET_BUCKET_WEBSITE
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::GET_BUCKET_LIFECYCLE
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::GET_OBJECT_ACL
        Object.get_object_acl(req.bucket, req.object, response)
      when Request::GET_OBJECT_META
        Object.get_object_meta(req.bucket, req.object, request, response)
      when Request::GET_SYMLINK
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::GET_OBJECT
        Object.get_object(req, request, response)
      when Request::GET_LIST_MULTIPART_UPLOADS
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::GET_LIST_PARTS
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      else 
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
      end #case
    rescue
      OssResponse.response_error(response, ErrorCode::INTERNAL_ERROR)
    end # do_GET

    def do_PUT(request, response)
      req = OssEmulator::Request.new(request)
      req.parse()

      case req.cmd
      when Request::PUT_BUCKET
        Bucket.create_bucket(req.bucket, request, response)
      when Request::PUT_BUCKET_ACL
        Bucket.put_bucket_acl(response)
      when Request::PUT_BUCKET_LOGGING
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::PUT_BUCKET_WEBSITE
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::PUT_BUCKET_REFERER
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::PUT_BUCKET_LIFECYCLE
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::PUT_OBJECT
        Object.put_object(req.bucket, req.object, request, response)
      when Request::PUT_OBJECT_ACL
        Object.put_object_acl(req.bucket, req.object, request, response)
      when Request::PUT_SYMLINK
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::PUT_UPLOAD_PART
        Multipart.upload_part(req, req.query_parser, request, response)
      when Request::PUT_UPLOAD_PART_COPY
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::PUT_COPY_OBJECT
        Object.copy_object(req.src_bucket, req.src_object, req.bucket, req.object, request, response)
      else 
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
      end #case
    rescue
      OssResponse.response_error(response, ErrorCode::INTERNAL_ERROR)
    end # do_PUT

    def do_POST(request, response)
      req = OssEmulator::Request.new(request)
      req.parse()

      case req.cmd
      when Request::POST_INIT_MULTIPART_UPLOAD 
        Multipart.initiate_multipart_upload(req.bucket, req.object, response)
      when Request::POST_APPEND_OBJECT  
        Object.append_object(req.bucket, req.object, request, req.query_parser['position'].first, response)
      when Request::POST_COMPLETE_MULTIPART_UPLOAD  
        Multipart.complete_multipart_upload(req, request, response)
      when Request::DELETE_MULTIPLE_OBJECTS
        Object.delete_multiple_objects(req.bucket, request, response)
      when Request::POST_OBJECT
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      else 
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
      end #case
    rescue
      OssResponse.response_error(response, ErrorCode::INTERNAL_ERROR)
    end # do_POST

    def do_DELETE(request, response)
      req = OssEmulator::Request.new(request)
      req.parse()

      case req.cmd
      when Request::DELETE_BUCKET
        Bucket.delete_bucket(req.bucket, response)
      when Request::DELETE_BUCKET_LOGGING
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::DELETE_BUCKET_WEBSITE
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::DELETE_BUCKET_LIFECYCLE
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::DELETE_OBJECT
        Object.delete_object(req.bucket, req.object, response)
      when Request::DELETE_ABORT_MULTIPART_UPLOAD
        OssResponse.response_error(response, ErrorCode::NOT_IMPLEMENTED)
      when Request::REQUEST_ERROR
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
      else 
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
      end #case
    rescue
      OssResponse.response_error(response, ErrorCode::INTERNAL_ERROR)
    end # do_DELETE

    def do_OPTIONS(request, response)
      super
      OssResponse.response_options(response)
    end # do_OPTIONS

  end #class Serverlet

  class Server
    def initialize(address, port, hostname, ssl_cert_path, ssl_key_path, extra_options={})
      @address = address
      @port = port
      @hostname = hostname
      @ssl_cert_path = ssl_cert_path
      @ssl_key_path = ssl_key_path
      webrick_config = {
        :BindAddress => @address,
        :Port => @port
      }
      if !@ssl_cert_path.to_s.empty?
        webrick_config.merge!(
          {
            :SSLEnable => true,
            :SSLCertificate => OpenSSL::X509::Certificate.new(File.read(@ssl_cert_path)),
            :SSLPrivateKey => OpenSSL::PKey::RSA.new(File.read(@ssl_key_path))
          }
        )
      end

      if extra_options[:quiet]
        begin
          webrick_config.merge!(
            :Logger => WEBrick::Log.new("/dev/null"),
            :AccessLog => []
          )
        rescue
          webrick_config.merge!(
            :Logger => WEBrick::Log.new(File.open(File::NULL, 'w')),
            :AccessLog => []
          )
        end
      end

      @server = WEBrick::HTTPServer.new(webrick_config)
    end

    def serve
      @server.mount "/", Servlet
      shutdown = proc { @server.shutdown }
      trap "INT", &shutdown
      trap "TERM", &shutdown
      @server.start
    end

    def shutdown
      @server.shutdown
    end
  end #class Server 

end #module
