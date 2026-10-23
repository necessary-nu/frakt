//! Thin wrappers over Cronet's `UrlRequest.Builder`.

use jni::JNIEnv;
use jni::objects::{JObject, JValue};

pub struct UrlRequestBuilder<'a, 'e> {
    env: &'e mut JNIEnv<'a>,
    builder: JObject<'a>,
}

impl<'a, 'e> UrlRequestBuilder<'a, 'e> {
    pub fn new(
        env: &'e mut JNIEnv<'a>,
        engine: &JObject,
        url: &str,
        callback: &JObject,
        executor: &JObject,
    ) -> Result<Self, jni::errors::Error> {
        let url = env.new_string(url)?;
        let builder = env
            .call_method(
                engine,
                "newUrlRequestBuilder",
                "(Ljava/lang/String;Lorg/chromium/net/UrlRequest$Callback;Ljava/util/concurrent/Executor;)Lorg/chromium/net/UrlRequest$Builder;",
                &[
                    JValue::Object(&url),
                    JValue::Object(callback),
                    JValue::Object(executor),
                ],
            )?
            .l()?;
        Ok(Self { env, builder })
    }

    pub fn set_http_method(&mut self, method: &str) -> Result<&mut Self, jni::errors::Error> {
        let method = self.env.new_string(method)?;
        self.env.call_method(
            &self.builder,
            "setHttpMethod",
            "(Ljava/lang/String;)Lorg/chromium/net/UrlRequest$Builder;",
            &[JValue::Object(&method)],
        )?;
        Ok(self)
    }

    pub fn add_header(&mut self, name: &str, value: &str) -> Result<&mut Self, jni::errors::Error> {
        let name = self.env.new_string(name)?;
        let value = self.env.new_string(value)?;
        self.env.call_method(
            &self.builder,
            "addHeader",
            "(Ljava/lang/String;Ljava/lang/String;)Lorg/chromium/net/UrlRequest$Builder;",
            &[JValue::Object(&name), JValue::Object(&value)],
        )?;
        Ok(self)
    }

    /// Add a header that `addHeader` would refuse.
    ///
    /// Cronet's `addHeader` silently drops `Accept-Encoding`
    /// (crbug.com/581399) and always asks for compressed bodies, which it then
    /// decodes, so `Content-Length` and `Content-Range` no longer describe the
    /// bytes delivered. This appends to the builder's private header list
    /// instead; Chromium only adds its own `Accept-Encoding` when none is set.
    /// Returns `Ok(false)`, leaving Cronet's default, if the Cronet
    /// implementation has no such list.
    pub fn force_header(&mut self, name: &str, value: &str) -> Result<bool, jni::errors::Error> {
        let headers =
            match self
                .env
                .get_field(&self.builder, "mRequestHeaders", "Ljava/util/ArrayList;")
            {
                Ok(headers) => headers.l()?,
                Err(_) => {
                    self.env.exception_clear()?;
                    return Ok(false);
                }
            };
        if headers.is_null() {
            return Ok(false);
        }
        let name = self.env.new_string(name)?;
        let value = self.env.new_string(value)?;
        let entry = self.env.new_object(
            "java/util/AbstractMap$SimpleEntry",
            "(Ljava/lang/Object;Ljava/lang/Object;)V",
            &[JValue::Object(&name), JValue::Object(&value)],
        )?;
        self.env.call_method(
            &headers,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&entry)],
        )?;
        Ok(true)
    }

    pub fn set_upload_data_provider(
        &mut self,
        provider: &JObject,
        executor: &JObject,
    ) -> Result<&mut Self, jni::errors::Error> {
        self.env.call_method(
            &self.builder,
            "setUploadDataProvider",
            "(Lorg/chromium/net/UploadDataProvider;Ljava/util/concurrent/Executor;)Lorg/chromium/net/UrlRequest$Builder;",
            &[JValue::Object(provider), JValue::Object(executor)],
        )?;
        Ok(self)
    }

    /// Use the builder's env for other JNI work while building.
    pub fn with_env<T>(&mut self, f: impl FnOnce(&mut JNIEnv<'a>) -> T) -> T {
        f(self.env)
    }

    pub fn build(self) -> Result<JObject<'a>, jni::errors::Error> {
        self.env
            .call_method(
                &self.builder,
                "build",
                "()Lorg/chromium/net/UrlRequest;",
                &[],
            )?
            .l()
    }
}
