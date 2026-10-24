// The WorkManager worker behind frakt::background on Android. A host app
// includes this module (or the AAR it builds) so the worker is in the app's
// own dex, where WorkManager can recreate it after the process has died.
plugins {
    id("com.android.library")
}

android {
    namespace = "se.brendan.frakt.background"
    compileSdk = 36

    defaultConfig {
        minSdk = 23
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    implementation("androidx.work:work-runtime:2.10.5")
}
