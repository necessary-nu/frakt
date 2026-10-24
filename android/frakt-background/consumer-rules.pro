# frakt calls these through JNI by name.
-keep class se.brendan.frakt.background.FraktBackground { public static *; }
# WorkManager instantiates the worker by class name.
-keep class se.brendan.frakt.background.FraktDownloadWorker { <init>(...); }
