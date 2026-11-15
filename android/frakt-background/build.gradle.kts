// The WorkManager worker behind frakt::background on Android. A host app
// includes this module, or depends on nu.necessary:frakt-background, so the
// worker is in the app's own dex, where WorkManager can recreate it after
// the process has died.
plugins {
    id("com.android.library")
    `maven-publish`
}

// The artifact is versioned with the crate whose Rust half calls it, so a
// frakt release and its worker carry one version; -Pversion overrides it.
val cargoVersion = Regex("""(?ms)^\[package\].*?^version\s*=\s*"([^"]+)"""")
    .find(projectDir.resolve("../../Cargo.toml").readText())
    ?.groupValues?.get(1)
    ?: error("frakt's Cargo.toml names no package version")

// Group and name are also what a composite build matches the coordinates
// nu.necessary:frakt-background against.
group = "nu.necessary"
version = providers.gradleProperty("version").getOrElse(cargoVersion)

android {
    namespace = "nu.necessary.frakt.background"
    compileSdk = 36

    defaultConfig {
        minSdk = 23
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    publishing {
        singleVariant("release")
    }
}

dependencies {
    implementation("androidx.work:work-runtime:2.10.5")
}

publishing {
    publications {
        register<MavenPublication>("release") {
            artifactId = "frakt-background"
            // AGP creates the component only once the project is evaluated.
            afterEvaluate {
                from(components["release"])
            }
            pom {
                name = "frakt-background"
                description = "The WorkManager worker behind frakt's background downloads on Android"
                url = "https://github.com/necessary-nu/frakt"
                licenses {
                    license {
                        name = "MIT"
                        url = "https://opensource.org/license/mit"
                    }
                    license {
                        name = "Apache-2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }
                scm {
                    url = "https://github.com/necessary-nu/frakt"
                }
            }
        }
    }
}
