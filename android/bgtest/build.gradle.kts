// An app that exercises frakt::background on a device or emulator; see
// run-emulator-test.sh. Not shipped.
plugins {
    id("com.android.application")
}

android {
    namespace = "se.brendan.frakt.bgtest"
    compileSdk = 36

    defaultConfig {
        applicationId = "se.brendan.frakt.bgtest"
        minSdk = 26
        targetSdk = 36
        versionCode = 1
        versionName = "1"
        ndk {
            abiFilters += "arm64-v8a"
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    implementation(project(":frakt-background"))
}
