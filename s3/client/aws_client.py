import boto3
import io


class AwsClient:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id="6cba959a9b2a405db52de505c1de1033",
            aws_secret_access_key="0acf210f6332441484549127b48ffc68",
            endpoint_url="https://s3.bhs.io.cloud.ovh.net/",
        )

    def upload_file(self, data: bytes, bucket: str, prefix: str):
        data = io.BytesIO(data)
        response = self.s3.put_object(Bucket=bucket, Key=prefix, Body=data)

    def download_file(self, bucket: str, prefix: str) -> bytes:
        response = self.s3.get_object(Bucket=bucket, Key=prefix)
        data = response["Body"].read()
        return data

    def delete_file(self, bucket, prefix) -> dict:
        response = self.s3.delete_object(Bucket=bucket, Key=prefix)
        return response
    
    def delete_folder(self, bucket, prefix) -> dict:
        """
        Delete a "folder" (prefix) and all its contents in an S3 bucket.

        Args:
        - bucket_name (str): The name of the S3 bucket.
        - folder_prefix (str): The prefix representing the "folder" to be deleted.

        Returns:
        - dict: A dictionary containing the response from the `delete_objects` API call.
        """
        # List objects under the prefix
        objects_to_delete = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Extract object keys to delete
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete.get('Contents', [])]}

        # Delete the objects
        response = self.s3.delete_objects(Bucket=bucket, Delete=delete_keys)

        return response
    
    def list_files(self, bucket, prefix) -> list:
        response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        res = [contents['Key'] for contents in response['Contents']]
        return res
    
    def list_folders(self,bucket_name, prefix) -> list:
        """
        List "folders" (common prefixes) inside a prefix in an S3 bucket.

        Args:
        - bucket_name (str): The name of the S3 bucket.
        - prefix (str): The prefix representing the "folder" whose subfolders to list.

        Returns:
        - list: A list of "folders" (common prefixes) inside the specified prefix.
        """
        if not prefix.endswith('/'):
            prefix += '/'

        # List objects using the delimiter to get only "folders"
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

        # Extract common prefixes (subfolders)
        common_prefixes = response.get('CommonPrefixes', [])

        # Extract folder names
        folder_names = [prefix.get('Prefix') for prefix in common_prefixes]

        return folder_names
    
    def change_folder_loc(self, bucket, source, destination) -> dict:
        """
        Copy a "folder" (prefix) and all its contents in an S3 bucket and delete the original.
        """
        if not source.endswith('/'):
            source += '/'
        if not destination.endswith('/'):
            destination += '/'

        keys = self.list_files(bucket, prefix=source)
        for key in keys:
            new_key = key.replace(source, destination, 1)  # Replace source with destination in key
            copy_source = {'Bucket': bucket, 'Key': key}
            self.s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
            self.s3.delete_object(Bucket=bucket, Key=key)
    
    def copy_folder(self, bucket, source, destination):
        """
        Copy a "folder" (prefix) and all its contents in an S3 bucket without deleting.
        """
        if not source.endswith('/'):
            source += '/'
        if not destination.endswith('/'):
            destination += '/'

        keys = self.list_files(bucket, prefix=source)
        for key in keys:
            new_key = key.replace(source, destination, 1)  # Replace source with destination in key
            copy_source = {'Bucket': bucket, 'Key': key}
            self.s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
    
    def change_file_name(self, bucket, prefix, new_prefix):
        copy_source = {'Bucket': bucket, 'Key': prefix}
        self.s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_prefix)
        self.s3.delete_object(Bucket=bucket, Key=prefix)


if __name__ == "__main__":
    aws = AwsClient()
    res = aws.list_folders("drivalake", "food/bronze/rappi")
