// The Android side of frakt's background downloads: frakt-background,
// which host apps include.
pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "frakt-android"
include(":frakt-background")
