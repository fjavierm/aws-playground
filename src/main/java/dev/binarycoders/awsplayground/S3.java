package dev.binarycoders.awsplayground;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.function.Function;

public class S3 {

    public static void main(String[] args) {
        final var bucketName = "dev.binarycoders.playground";
        final var objectKey = "empty-file";
        final var s3Client = S3Client.builder().build();

        System.out.println("Checking if bucket already exists...");
        if (bucketExists(s3Client, bucketName)) {
            System.out.println("Removing bucket content...");
            emptyBucket(s3Client, bucketName);

            System.out.println("Deleting bucket");
            s3Client.deleteBucket(builder -> builder.bucket(bucketName));
        }

        System.out.println("Creating bucket...");
        s3Client.createBucket(builder -> builder.bucket(bucketName));

        System.out.println("Adding an object to the bucket...");
        s3Client.putObject(builder -> builder.bucket(bucketName).key(objectKey), RequestBody.empty());

        System.out.println("Listing all objects in the bucket...");
        listObjects(s3Client, bucketName);

        System.out.println("Removing bucket and objects in it...");
        emptyBucket(s3Client, bucketName);
        s3Client.deleteBucket(builder -> builder.bucket(bucketName));
    }

    private static boolean bucketExists(final S3Client s3Client, final String bucketName) {
        final var listBucketsResponse = s3Client.listBuckets();

        return listBucketsResponse.hasBuckets()
            && listBucketsResponse.buckets().stream()
            .anyMatch(bucket -> bucket.name().equals(bucketName));
    }

    private static void emptyBucket(final S3Client s3Client, final String bucketName) {
        applyToObjects(s3Client, bucketName, (s3Object) -> s3Client.deleteObject(builder -> builder.bucket(bucketName).key(s3Object.key())));
    }

    private static void listObjects(final S3Client s3Client, final String bucketName) {
        applyToObjects(s3Client, bucketName, (s3Object) -> System.out.printf("\t%s%n", s3Object.key()));
    }

    private static void applyToObjects(final S3Client s3Client, final String bucketName, final Function<S3Object, Object> function) {
        var listObjectsRequest = ListObjectsRequest.builder().bucket(bucketName).build();
        var hasMore = false;

        do {
            var listObjectsResponse = s3Client.listObjects(listObjectsRequest);
            for (final S3Object object : listObjectsResponse.contents()) {
                function.apply(object);
            }

            hasMore = listObjectsResponse.isTruncated();
            if (hasMore) {
                listObjectsRequest = ListObjectsRequest.builder().marker(listObjectsResponse.nextMarker()).build();
            }
        } while (hasMore);
    }
}
