export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs" && process.env.NEXT_PHASE !== "phase-production-build") {
    // Warm the S3 transport and temporary credentials before the first sign-in.
    // Authorization still reads the current account record on every request.
    void import("./features/auth/storage").then(({readAccounts}) => readAccounts()).catch(() => {});
  }
  if (process.env.NEXT_RUNTIME === "nodejs" && process.env.TRACKER_DATA_SOURCE === "s3" && process.env.TRACKER_S3_AUTO_REFRESH !== "0") {
    const { startS3Refresh } = await import("./features/s3/auto-refresh");
    startS3Refresh();
  }
}
