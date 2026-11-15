# frakt calls these through JNI by name.
-keep class nu.necessary.frakt.background.FraktBackground { public static *; }
# WorkManager instantiates the worker by class name.
-keep class nu.necessary.frakt.background.FraktDownloadWorker { <init>(...); }
