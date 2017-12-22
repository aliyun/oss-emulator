require 'emulator/config'

module OssEmulator
  class Request
    PUT_BUCKET = "PUT_BUCKET"
    PUT_BUCKET_ACL = "PUT_BUCKET_ACL"
    PUT_BUCKET_LOGGING = "PUT_BUCKET_LOGGING"
    PUT_BUCKET_REFERER = "PUT_BUCKET_REFERER"
    PUT_BUCKET_WEBSITE = "PUT_BUCKET_WEBSITE"
    PUT_BUCKET_LIFECYCLE = "PUT_BUCKET_LIFECYCLE" 

    PUT_OBJECT = "PUT_OBJECT"
    PUT_OBJECT_ACL = "PUT_OBJECT_ACL"
    PUT_SYMLINK = "PUT_SYMLINK" 
    PUT_UPLOAD_PART = "PUT_UPLOAD_PART"
    PUT_UPLOAD_PART_COPY = "PUT_UPLOAD_PART_COPY"
    PUT_COPY_OBJECT = "PUT_COPY_OBJECT"

    LIST_BUCKETS = "LIST_BUCKETS" 
    GET_BUCKET = "GET_BUCKET"
    GET_BUCKET_ACL = "GET_BUCKET_ACL"
    GET_BUCKET_INFO = "GET_BUCKET_INFO"
    GET_BUCKET_LOCATION = "GET_BUCKET_LOCATION"
    GET_BUCKET_LOGGING = "GET_BUCKET_LOGGING"
    GET_BUCKET_REFERER = "GET_BUCKET_REFERER"
    GET_BUCKET_WEBSITE = "GET_BUCKET_WEBSITE"
    GET_BUCKET_LIFECYCLE = "GET_BUCKET_LIFECYCLE" 

    GET_OBJECT = "GET_OBJECT"
    GET_OBJECT_ACL = "GET_OBJECT_ACL"
    GET_OBJECT_META = "GET_OBJECT_META"
    GET_SYMLINK = "GET_SYMLINK" 

    GET_LIST_MULTIPART_UPLOADS = "GET_LIST_MULTIPART_UPLOADS"
    GET_LIST_PARTS = "GET_LIST_PARTS"

    HEAD_OBJECT = "HEAD_OBJECT"

    DELETE_BUCKET = "DELETE_BUCKET"
    DELETE_BUCKET_LOGGING = "DELETE_BUCKET_LOGGING"
    DELETE_BUCKET_WEBSITE = "DELETE_BUCKET_WEBSITE"
    DELETE_BUCKET_LIFECYCLE = "DELETE_BUCKET_LIFECYCLE"
    DELETE_ABORT_MULTIPART_UPLOAD = "DELETE_ABORT_MULTIPART_UPLOAD"

    DELETE_OBJECT = "DELETE_OBJECT"
    DELETE_MULTIPLE_OBJECTS = "DELETE_MULTIPLE_OBJECTS"

    POST_RESTORE_OBJECT  = "POST_RESTORE_OBJECT"

    POST_APPEND_OBJECT  = "POST_APPEND_OBJECT"
    POST_INIT_MULTIPART_UPLOAD  = "POST_INIT_MULTIPART_UPLOAD"
    POST_COMPLETE_MULTIPART_UPLOAD  = "POST_COMPLETE_MULTIPART_UPLOAD"
    POST_OBJECT  = "POST_OBJECT"
    POST_ELSE  = "POST_ELSE"

    REQUEST_ERROR = "REQUEST_ERROR"

    attr_accessor :request, :host, :is_path_style, :path, :query, :query_parser, :method, :cmd, 
                  :bucket, :bucket_name, :object, :src_bucket, :src_object, :path_length

    def initialize(request)
      @request = request
      @query = @request.query
      @query_parser = CGI::parse(request.request_uri.query || "")
      @path = @request.path
      @path_length = @request.path.size
      @cmd ||= ''
      @bucket ||= ''
      @object ||= ''
      @src_bucket ||= ''
      @src_object ||= ''
    end

    def validate_request()
      return if @request.nil?
      return if not @request.header.has_key?('expect')
      @request.continue if @request.header['expect'].first=='100-continue'
    end

    def parse()
      host_header= @request["Host"]
      @host = host_header.split(':')[0]
      if @host.include?(',')
         @host = @host.split(',')[0]
      end
      @is_path_style = true

      if !Config.hostnames.include?(@host) && !(IPAddr.new(@host) rescue nil)
        @bucket = @host.split(".")[0]
        @is_path_style = false
        Log.info("Request.parse : @is_path_style=false, Config.hostnames=#{Config.hostnames}, @host=#{Config.host}", "blue")
      end

      @method = @request.request_method

      case @method
      when 'PUT'
        parse_put()
      when 'GET','HEAD'
        parse_get()
      when 'DELETE'
        parse_delete()
      when 'POST'
        parse_post()
      else
        Log.raise("Request.parse : Unknown Request Method")
      end

      validate_request()
      inspect_request()
    end

    def parse_put()
      if @path == "/"
        if @bucket
          @cmd = Request::PUT_BUCKET
        end
      else
        if @is_path_style
          elems = @path[1,@path_length].split("/")
          @bucket = elems[0]
          if elems.size == 1
            if @request.request_line =~ /\?acl/
              @cmd = Request::PUT_BUCKET_ACL
            elsif @request.request_line =~ /\?logging/
              @cmd = Request::PUT_BUCKET_LOGGING
            elsif @request.request_line =~ /\?website/
              @cmd = Request::PUT_BUCKET_WEBSITE
            elsif @request.request_line =~ /\?referer/
              @cmd = Request::PUT_BUCKET_REFERER
            elsif @request.request_line =~ /\?lifecycle/
              @cmd = Request::PUT_BUCKET_LIFECYCLE
            else
              if @request.header.include?('x-oss-acl')
                @cmd = Request::PUT_BUCKET_ACL
              else
                @cmd = Request::PUT_BUCKET
              end
            end
          else
            if @request.request_line =~ /\?acl/
              @cmd = Request::PUT_OBJECT_ACL
            elsif @request.request_line =~ /\?symlink/
              @cmd = Request::PUT_SYMLINK
            elsif @request.request_line =~ /\?partNumber=/  
              if @request.header.include?('x-oss-copy-source')
                @cmd = Request::PUT_UPLOAD_PART_COPY
              else
                @cmd = Request::PUT_UPLOAD_PART
              end
            else
              if @request.header.include?('x-oss-copy-source')
                @cmd = Request::PUT_COPY_OBJECT
              else
                @cmd = Request::PUT_OBJECT
              end
            end

            @object = elems[1,elems.size].join('/')
          end
        else
          if @request.request_line =~ /\?acl/
            @cmd = Request::PUT_BUCKET_ACL
          elsif @request.request_line =~ /\?logging/
            @cmd = Request::PUT_BUCKET_LOGGING
          elsif @request.request_line =~ /\?website/
            @cmd = Request::PUT_BUCKET_WEBSITE
          elsif @request.request_line =~ /\?referer/
            @cmd = Request::PUT_BUCKET_REFERER
          elsif @request.request_line =~ /\?lifecycle/
            @cmd = Request::PUT_BUCKET_LIFECYCLE
          else
            if @request.header.include?('x-oss-acl')
              @cmd = Request::PUT_BUCKET_ACL
            else
              @cmd = Request::PUT_BUCKET
            end
          end

          @cmd = @request.path[1..-1]
        end
      end

      # Also parse x-oss-copy-source-range:bytes=first-last header for multipart copy
      copy_source = @request.header["x-oss-copy-source"]
      if copy_source && copy_source.size == 1
        src_elems   = copy_source.first.split("/")
        root_offset = src_elems[0] == "" ? 1 : 0
        @src_bucket = src_elems[root_offset]
        @src_object = src_elems[1 + root_offset,src_elems.size].join("/")
        @cmd = Request::PUT_COPY_OBJECT
      end
    end

    def parse_get()
      @path_length = @path.size
      if @path == "/" && @is_path_style
        if @request.request_line =~ /\?uploads/
          @cmd = Request::GET_LIST_MULTIPART_UPLOADS
        elsif @request.request_line =~ /\?logging/
          @cmd = Request::GET_BUCKET_LOGGING
        elsif @request.request_line =~ /\?website/
          @cmd = Request::GET_BUCKET_WEBSITE
        elsif @request.request_line =~ /\?refer/
          @cmd = Request::GET_BUCKET_REFER
        elsif @request.request_line =~ /\?lifecycle/
          @cmd = Request::GET_BUCKET_LIFECYCLE
        else 
          @cmd = Request::LIST_BUCKETS
        end
      else
        if @is_path_style
          elems = @path[1,@path_length].split("/")
          @bucket = elems[0]
        else
          elems = @path.split("/")
        end

        if elems.size < 2  # bucket only
          if query["acl"] == ""
            @cmd = Request::GET_BUCKET_ACL
          elsif query["location"] == ""
            @cmd = Request::GET_BUCKET_LOCATION
          elsif query["bucketInfo"] == ""
            @cmd = Request::GET_BUCKET_INFO
          else
            @cmd = Request::GET_BUCKET
          end
        else  # bucket && object 
          if query["acl"] == ""
            @cmd = Request::GET_OBJECT_ACL
          elsif query["objectMeta"] == "" || @request.request_line =~ /\?objectMeta/
            @cmd = Request::GET_OBJECT_META
          elsif @request.request_line =~ /\?symlink/
            @cmd = Request::GET_SYMLINK
          elsif @request.request_line =~ /\?uploadId/
            @cmd = Request::GET_LIST_PARTS
          else
            if @method=="HEAD"
              @cmd = Request::HEAD_OBJECT
            else
              @cmd = Request::GET_OBJECT
            end
          end
          @object = elems[1,elems.size].join('/')
        end
      end
    end

    def parse_delete()
      if @path == "/" && @is_path_style
        @cmd = Request::REQUEST_ERROR
      else
        if @is_path_style
          elems = @path[1,@path_length].split("/")
          @bucket = elems[0]
        else
          elems = @path.split("/")
        end

        if elems.size == 0
          Log.raise("Request.parse_delete : Unsupported Operation. ")
        elsif elems.size == 1
          if @request.request_line =~ /\?logging/
            @cmd = Request::DELETE_BUCKET_LOGGING
          elsif @request.request_line =~ /\?website/
            @cmd = Request::DELETE_BUCKET_WEBSITE
          elsif @request.request_line =~ /\?lifecycle/
            @cmd = Request::DELETE_BUCKET_LIFECYCLE
          else
            @cmd = Request::DELETE_BUCKET
          end
        else
          # AbortMultipartUpload
          if @request.request_line =~ /\?uploadId/  
            @cmd = Request::DELETE_ABORT_MULTIPART_UPLOAD
          else
            @cmd = Request::DELETE_OBJECT
          end
          @object = elems[1,elems.size].join('/')
        end
      end
    end

    def parse_post()
      @path_length = @path.size

      if @is_path_style
        elems = @path[1, @path_length].split("/")
        @bucket = elems[0]
        @object = elems[1..-1].join('/') if elems.size >= 2
      else
        @object = @path[1..-1]
      end

      if @query_parser.has_key?('uploads')  # InitiateMultipartUpload
        @cmd = Request::POST_INIT_MULTIPART_UPLOAD
      elsif @query_parser.has_key?('append')  # AppendObject
        @cmd = Request::POST_APPEND_OBJECT
      elsif @query_parser.has_key?('uploadId')  # CompleteMultipartUpload
        @cmd = Request::POST_COMPLETE_MULTIPART_UPLOAD
      elsif @request.request_line =~ /\?delete/
        @cmd = Request::DELETE_MULTIPLE_OBJECTS
      elsif @query_parser.has_key?('restore')  # RestoreObject
        @cmd = Request::POST_RESTORE_OBJECT
      elsif request.content_type =~ /^multipart\/form-data; boundary=(.+)/
        @cmd = Request::POST_OBJECT
      else
        @cmd = Request::POST_ELSE
      end
    end

    def get_bucket_from_header_host()
      host_header= @request["Host"]
      @bucket_name = host_header.split('.')[0]
    end

    def inspect_request()
      Log.info("Request.inspect_request : Oss Request Command Type : #{@cmd}", "magenta_bg")
      Log.info("Request.inspect_request : Inspect Request Begin")
      Log.info("Request.inspect_request : HOST: #{@host}")
      Log.info("Request.inspect_request : Path: #{@path}")
      Log.info("Request.inspect_request : Path_length: #{@path_length}")
      Log.info("Request.inspect_request : Query: #{@query}")
      Log.info("Request.inspect_request : QueryParser: #{@query_parser}")
      Log.info("Request.inspect_request : Is @path Style: #{@is_path_style}")
      Log.info("Request.inspect_request : Request Method: #{@method}")
      Log.info("Request.inspect_request : Type Cmd: #{@cmd}")
      Log.info("Request.inspect_request : Bucket: #{@bucket}")
      Log.info("Request.inspect_request : Object: #{@object}")
      Log.info("Request.inspect_request : Src Bucket: #{@src_bucket}")
      Log.info("Request.inspect_request : Src Object: #{@src_object}")
      Log.info("Request.inspect_request : Inspect Request End ")
      dump_request()
    end

    def dump_request()
      Log.info("Request.dump_request : Dump Request Begin ")
      Log.info("Request.dump_request : #{@request.request_method}")
      Log.info("Request.dump_request : #{@request.path}")
      @request.each do |k,v|
        Log.info("#{k}:#{v}")
      end
      Log.debug("Request.dump_request : request.header : #{request.header}")
      Log.info("Request.dump_request : Dump Request End ")
    end

  end # class

end # module