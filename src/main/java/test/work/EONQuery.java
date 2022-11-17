package test.work;

import java.io.FileInputStream;
import java.net.URL;
import java.util.Properties;

import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/*
 * Test out EON tokens and queries.  
 * This WILL NOT WORK OUTSIDE THE FAA NETWORK!!!!
 */
public class EONQuery
{
    private static String configFile = "/TEST_DATA/conf/work-tester.conf";
    private static Properties cProperties;
    
    public static void main(String[] args) throws Exception
    {
        for (int i = 0; i < args.length; ++i)
        {
            if (args[i].equals("--conf"))
            {
                configFile = args[++i];
            }
        }
        EONQuery eon = new EONQuery();
        eon.loadConfig();
        eon.getTokenForEONData();
    }
    public EONQuery()
    {
        
    }
    public String getTokenForEONData() throws Exception
    {
        String user = cProperties.getProperty("USER");
        String pass = cProperties.getProperty("PASSWORD");
        String tokenURL = cProperties.getProperty("TOKEN_URL");
        
        System.out.println(String.format("Connecting to %s with user [%s]", tokenURL, user));
        
        URL muleURL = new URL(tokenURL);
        OkHttpClient client = new OkHttpClient.Builder().build();
        FormBody body = new FormBody.Builder()
                .addEncoded("username", user)
                .addEncoded("password", pass)
                .addEncoded("client", "requestip")
                .addEncoded("expiration", "60")
                .addEncoded("f", "pjson")
                .build();
        Request request = new Request.Builder()
                .url(muleURL)
                .method("POST", body)
                .addHeader("Content-Type", "application/x-www-form-urlencoded")
                .build();
        Response response = client.newCall(request).execute();
        String resBody = response.body().string();
        response.close();
        System.out.println(resBody);
        
        return "";
    }
    private void loadConfig()
    {
        try
        {
            cProperties = new Properties();
            cProperties.load(new FileInputStream(configFile));
        }
        catch (Exception e) { e.printStackTrace(); System.exit(11);}
    }
}
