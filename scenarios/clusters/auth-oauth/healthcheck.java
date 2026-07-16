// Tiny HTTP readiness probe for the mock-oauth2 container (distroless JVM image —
// no shell is available, so CMD-SHELL healthchecks cannot be used).
// Run with: java --source 11 /health/healthcheck.java
public class healthcheck {
    public static void main(String[] args) throws Exception {
        // SERVER_HOSTNAME=mock-oauth2 causes the server to bind to the container's
        // hostname (not 127.0.0.1), so the probe must use the container hostname.
        var url = new java.net.URL(
            "http://mock-oauth2:8080/default/.well-known/openid-configuration");
        var con = (java.net.HttpURLConnection) url.openConnection();
        con.setConnectTimeout(3000);
        con.setReadTimeout(3000);
        int code = con.getResponseCode();
        System.exit(code == 200 ? 0 : 1);
    }
}
