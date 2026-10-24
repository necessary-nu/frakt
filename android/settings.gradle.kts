// The Android side of frakt's background downloads (frakt-background, which
// host apps include) and the app that exercises it on a device (bgtest).
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
include(":frakt-background", ":bgtest")
