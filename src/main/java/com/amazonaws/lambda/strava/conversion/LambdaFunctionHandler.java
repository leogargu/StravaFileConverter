package com.amazonaws.lambda.strava.conversion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    public LambdaFunctionHandler() {}

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }
    
    // Gets the extension of a file, given by its relative or full path
	public static String get_extension(String path) throws Exception {
		int lastIndexOf = path.lastIndexOf(".");
		if (lastIndexOf == -1) {
			System.out.println("No extension");
		    throw new Exception();
		}
		String extension = path.substring(lastIndexOf+1);
		return extension;
	}
    
	// Given a full path to a file, it returns the key it should be saved into s3 as
	// filename = path to already converted file (as it will be uploaded)
	// It should exist
	public static String get_output_key(String filename) throws Exception{
		File file = new File(filename);
		String name_of_file = file.getName();
		
		String extension = get_extension(filename);
		if(extension.equalsIgnoreCase("fit")){
			return "fixed/" + name_of_file;
		}else if(extension.equalsIgnoreCase("csv")){
			return "converted/" + name_of_file;
		}else{
			System.out.println("Unrecognised extension");
			throw new Exception();
		}
	}

	// Converts the file given by filename. Returns the path to the converted file
    public static String convert_file(String filename, LambdaLogger logger) throws Exception {
		try {
			// filename contains path
			String[] name_and_extension = filename.split("\\.");
			if(name_and_extension.length != 2) {
				System.out.println("Incorrect file name");
				throw new Exception();
			}
			String path_and_name = name_and_extension[0];
			
			String[] path_and_name_splitted = path_and_name.split("/");
			int name_idx = path_and_name_splitted.length -1;
			String name = path_and_name_splitted[name_idx];
			
			String extension = name_and_extension[1];
			String flag = "";
			String output = "/tmp/";
			
	        logger.log("name: " + path_and_name);
	        logger.log("name: " + name);
	        logger.log("extension: " + extension);

			
			if(extension.equalsIgnoreCase("fit")){
				System.out.println("Converting fit file to csv");
				flag = "-b";
				output = output + name + ".csv";
			} else if(extension.equalsIgnoreCase("csv")) {
				System.out.println("Converting csv file to fit");
				flag = "-c";
				output = output + name + ".fit";
			} else{
				System.out.println("Unknown file extension");
				throw new Exception();
			}

            logger.log("Calling ProcessBuilder");

			ProcessBuilder pBuilder = new ProcessBuilder("java", "-jar", "lib/FitCSVTool.jar", flag, filename, output);
            logger.log("Starting process");

			final Process p = pBuilder.start();

			// Redirect output, otherwise we won't see it
			BufferedReader br=new BufferedReader(
		            new InputStreamReader(
		               p.getInputStream()));
		            String line;
		            while((line=br.readLine())!=null){
		            	logger.log(line);
		            }
		            
		    logger.log("Output redirected, process was started");
		    
		    return output;

		  } catch (IOException e) {
			e.printStackTrace();
			return "Error";
		  }
	}
    
    // Download the object given by key from the bucket. Saves it to /tmp (only writeable location in lambda)
    public String download_file_from_s3(String bucket, String key, LambdaLogger logger) throws Exception {
    	logger.log("Downloading " + key + " from bucket " + bucket);
    	
    	File the_file = new File(key);
    	String filename = the_file.getName();
    	
    	logger.log("Filename: " + filename);

        S3Object o = s3.getObject(bucket, key);
        S3ObjectInputStream s3is = o.getObjectContent();
        String lambda_path = "/tmp/" + filename;
        FileOutputStream fos = new FileOutputStream(new File(lambda_path));
        byte[] read_buf = new byte[1024];
        int read_len = 0;
        while ((read_len = s3is.read(read_buf)) > 0) {
            fos.write(read_buf, 0, read_len);
        }
        s3is.close();
        fos.close();
        
        File tmp = new File(lambda_path);
        boolean exists = tmp.exists();
        logger.log("Does " + lambda_path + " exist?: " + exists);
        if(!exists){
        	logger.log("Error downloading the file from s3");
        	throw new Exception();
        }
        return lambda_path;
    }
 

    @Override
    public String handleRequest(S3Event event, Context context) {
    	LambdaLogger logger = context.getLogger();
    	
        logger.log("Received event: " + event);

        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        
        logger.log("Bucket" + bucket);
        logger.log("Key: " + key);

        try {
        	// Download file from s3. Returns the path to the downloaded file
        	String lambda_path = download_file_from_s3(bucket, key, logger);

        	// Get metadata, as it should be transferred to the converted object
            S3Object o = s3.getObject(bucket, key);
            String original_name = o.getObjectMetadata().getUserMetaDataOf("Original_Name");
            String external_id = o.getObjectMetadata().getUserMetaDataOf("External_Id");
            String activity_id = o.getObjectMetadata().getUserMetaDataOf("Activity_Id");
            logger.log("Metadata - Original name: " + original_name);
            logger.log("Metadata - External Id: " + external_id);
            logger.log("Metadata - Activity Id: " + activity_id);
        	
        	// Convert the downloaded file.
            logger.log("Calling convert_file");
            String output_file = convert_file(lambda_path, logger);
            logger.log("convert_file call complete");
            logger.log("Output_file: "+ output_file);
            File tmp = new File(output_file);
            context.getLogger().log("Does " + output_file + " exist?: " + tmp.exists());
            
            // Upload the converted file to s3. Find out what its key should be first,
            // and prepare the metadata
            logger.log("Calling get_output_key");
            String upload_key = get_output_key(output_file);
            logger.log("Output key is: " + upload_key);
            
            logger.log("Calling put object request with metadata");
            PutObjectRequest request = new PutObjectRequest(bucket, upload_key, new File(output_file));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("Original_Name", original_name);
            metadata.addUserMetadata("External_Id", external_id);
            metadata.addUserMetadata("Activity_Id", activity_id);
            request.setMetadata(metadata);

            logger.log("Request made, now uploading file to s3");
            s3.putObject(request);
            logger.log("File uploaded to s3");

            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            return "Error";
        }
    }
}