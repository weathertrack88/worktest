package test.work;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Test out Java API for working with AWS S3.
 * 
 */
public class TestS3Connect
{
    private AmazonS3 s3client;
    private String bucketName = "eimdev1-weather";
    
    public static void main(String[] args) throws IOException
    {
        TestS3Connect s3test = new TestS3Connect();
//        s3test.listS3_a();      // Basic 
//        s3test.listS3_b();      // get ALL objects 
        s3test.listS3_c();      // Try getting data from specific key/folder under bucket
        // Download works 
//        s3test.downloadKey();       // Try downloading a file locally 
    }
    public TestS3Connect() 
    {
        /*
         * eimdev1-weather S3 access
         * accesskey = ...
         * secretkey= ...
         */
        AWSCredentials credentials = new BasicAWSCredentials(
                "...", 
                "..."
              );
        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-gov-west-1")
                .build();
    }
    public void listS3_a()
    {
        System.out.println("Test listing A");
        ObjectListing objectListing = s3client.listObjects(bucketName);
        System.out.println("Size: " + objectListing.getObjectSummaries().size());
        // Size will be maxed at 1000 
        // Prints all files found (1000)
//        for(S3ObjectSummary os : objectListing.getObjectSummaries()) 
//            System.out.println(os.getKey());
    }
    public void listS3_b()
    {
        // If there are a ton of files, dont do this...will not return 
        System.out.println("Test listing B");
        List<S3ObjectSummary> keyList = new ArrayList<S3ObjectSummary>();
        ObjectListing objects = s3client.listObjects(bucketName);
        keyList.addAll(objects.getObjectSummaries());

        while (objects.isTruncated()) 
        {
            objects = s3client.listNextBatchOfObjects(objects);
            keyList.addAll(objects.getObjectSummaries());
        }
        System.out.println("Getting ALL, size: " + keyList.size());
    }
    public void listS3_c()
    {
        System.out.println("Test listing C - list all files under prefix");
        String start = getCurrentTime();
        List<String> files = getObjectslistFromFolder(bucketName, "ciws-vil/wx/raw/netcdf/2017/01/03/");
        for (String ff: files)
        {
            System.out.println("VIL file: " + ff);
        }
        System.out.println("Found " + files.size() + " files");
        System.out.println("  Done C ..[ "+start + " end: " + getCurrentTime());
    }
    public List<String> getObjectslistFromFolder(String bucketName, String folderKey) 
    {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                                                   .withBucketName(bucketName)
                                                   .withPrefix(folderKey);
        List<String> keys = new ArrayList<>();
        ObjectListing objects = s3client.listObjects(listObjectsRequest);
        
        for (;;) 
        {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.size() < 1) 
                break;
            summaries.forEach(s -> keys.add(s.getKey()));
            objects = s3client.listNextBatchOfObjects(objects);
        }
         
        return keys;
    }
    public void downloadKey() throws IOException
    {
        System.out.println("Downloading file...");
        S3Object s3object = s3client.getObject(bucketName, "ciws-vil/wx/raw/netcdf/2017/01/03/20170103_v_000230_l_0000000.nc");
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        FileUtils.copyInputStreamToFile(inputStream, new File("/tmp/local-20170103_v_000230_l_0000000.nc"));
    }
    //! Get this out
    public static String getCurrentTime()
    {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        // Get current date time
        Date date = new Date();
        String timeString = dateFormat.format(date);
        return timeString;
    }
}
