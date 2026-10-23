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
