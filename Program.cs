using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Amazon.Glacier;
using Amazon.Glacier.Model;
using Amazon.Runtime;
using Amazon;

namespace MultipartUploadTool
{
    class Program
    {
        // Global Variables
        static string vaultName = "";
        static readonly long partSize = 1048576; // 100MB 
        static string ArchiveDescription = "";
        static string profileName = "";
        static string archiveFile = "";
        static int noOfFiles;
        static int uploadAttempt = 1;

        public static void Main(string[] args)
        {
            RedText("*BEFORE YOU CAN START, YOU NEED TO CREATE A PROFILE* \n");
            profileName = RegisterProfile();

            // Sets the region 
            string region = SetRegion();

            // Name of AWS Vault
            Console.WriteLine("\n Input Vault Name");
            vaultName = Console.ReadLine();

            // Upload description for the archive
            Console.WriteLine("\n Input Archive Description");
            ArchiveDescription = Console.ReadLine();

            // Retrieves the number of files to upload and the relevant file paths 
            List<string> archiveToUpload = new List<string>();
            archiveToUpload.AddRange(FilePath());

            // Loops until no files to upload are left
            for (int i = 0; i < archiveToUpload.Count; i++)
            {
                archiveFile = archiveToUpload[i];

                /* Passes In AWS Profile
                 * And the archive file path */
                AmazonGlacierClient(profileName, archiveFile, region);

            }

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Make sure to save the Archive ID");
            Console.ForegroundColor = ConsoleColor.White;

            Console.WriteLine("\n Press Enter to exit");
            Console.ReadLine();

        }

        // Method to create a profile for AWS Credentials
        public static string RegisterProfile()
        {
            Console.WriteLine("Input Profile Name");
            string profileName = Console.ReadLine();
            Console.WriteLine("Input Access Key");
            string accessKeyId = Console.ReadLine();
            Console.WriteLine("Input Secret Key");
            string secretKey = Console.ReadLine();

            // Registers profile using the Access Key and the Secret Key which is then stored to the profileName 
            Amazon.Util.ProfileManager.RegisterProfile(profileName, accessKeyId, secretKey);
            return profileName;
        }

        public static void AmazonGlacierClient(string profileName, string archiveToUpload, string region)
        {
            AmazonGlacierClient client;
            List<string> partChecksumList = new List<string>();
            var credentials = new StoredProfileAWSCredentials(profileName); // AWS Profile
            var newRegion = RegionEndpoint.GetBySystemName(region);
            try
            {
                // Connects to Amazon Glacier using your credentials and the specified region 
                using (client = new AmazonGlacierClient(credentials, newRegion))
                {

                    Console.WriteLine("Uploading an archive. \n");
                    string uploadId = InitiateMultipartUpload(client, vaultName);
                    partChecksumList = UploadParts(uploadId, client, archiveToUpload);
                    string archiveId = CompleteMPU(uploadId, client, partChecksumList, archiveToUpload);
                    Console.WriteLine("Archive ID: {0}", archiveId);
                }

                Console.WriteLine("Operation was successful.");

            }

            // If Glacier times out it will re attempt the upload 5 times
            catch (RequestTimeoutException)
            {
                uploadAttempt = +1;
                Console.WriteLine("Glacier Timed out while receiving the upload \n Upload Attempt " + uploadAttempt + " / 5");

                Console.WriteLine(" Upload Attempt " + uploadAttempt + " / 5");
                if (uploadAttempt < 5)
                {

                    uploadAttempt++;
                    AmazonGlacierClient(profileName, archiveToUpload, region);
                }
                else
                {
                    Console.WriteLine("\n Glacier timed out 5 times while receiving the upload. \n Please Restart the program and try again.");
                    Console.ReadLine();
                    System.Environment.Exit(1);

                }
            }

            catch (AmazonGlacierException e)
            {
                Console.WriteLine(e.Message);
            }

            catch (AmazonServiceException e) { Console.WriteLine(e.Message); }
            catch (Exception e) { Console.WriteLine(e.Message); }

        }

        public static List<String> FilePath()
        {
            List<string> vaultName = new List<string>();

            // Loop to stop incorrect datatype exception 
            bool input = true;
            while (input)
            {
                try
                {
                    Console.WriteLine("How Many files are you uploading? ");
                    noOfFiles = Convert.ToInt32(Console.ReadLine());

                    if (noOfFiles >= 1)
                    {
                        input = false;
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("Invalid Input");
                }
            }

            for (int i = 0; i < noOfFiles; i++)
            {
                var counter = i + 1;
                Console.WriteLine("Input File Path " + counter + ": ");
                vaultName.Add(Console.ReadLine());
            }


            return vaultName;
        }

        static string InitiateMultipartUpload(AmazonGlacierClient client, string vaultName)
        {
            InitiateMultipartUploadRequest initiateMPUrequest = new InitiateMultipartUploadRequest()
            {

                VaultName = vaultName,
                PartSize = partSize,
                ArchiveDescription = ArchiveDescription
            };

            InitiateMultipartUploadResponse initiateMPUresponse = client.InitiateMultipartUpload(initiateMPUrequest);

            return initiateMPUresponse.UploadId;
        }

        static List<string> UploadParts(string uploadID, AmazonGlacierClient client, string archiveToUpload)
        {
            List<string> partChecksumList = new List<string>();
            long currentPosition = 0;
            var buffer = new byte[Convert.ToInt32(partSize)];

            long fileLength = new FileInfo(archiveToUpload).Length;
            using (FileStream fileToUpload = new FileStream(archiveToUpload, FileMode.Open, FileAccess.Read))
            {
                while (fileToUpload.Position < fileLength)
                {
                    Stream uploadPartStream = GlacierUtils.CreatePartStream(fileToUpload, partSize);
                    string checksum = TreeHashGenerator.CalculateTreeHash(uploadPartStream);
                    partChecksumList.Add(checksum);

                    UploadMultipartPartRequest uploadMPUrequest = new UploadMultipartPartRequest()
                    {

                        VaultName = vaultName,
                        Body = uploadPartStream,
                        Checksum = checksum,
                        UploadId = uploadID
                    };
                    uploadMPUrequest.SetRange(currentPosition, currentPosition + uploadPartStream.Length - 1);
                    client.UploadMultipartPart(uploadMPUrequest);

                    currentPosition = currentPosition + uploadPartStream.Length;
                }
            }
            return partChecksumList;
        }

        static string CompleteMPU(string uploadID, AmazonGlacierClient client, List<string> partChecksumList, string archiveToUpload)
        {
            long fileLength = new FileInfo(archiveToUpload).Length;
            CompleteMultipartUploadRequest completeMPUrequest = new CompleteMultipartUploadRequest()
            {
                UploadId = uploadID,
                ArchiveSize = fileLength.ToString(),
                Checksum = TreeHashGenerator.CalculateTreeHash(partChecksumList),
                VaultName = vaultName
            };

            CompleteMultipartUploadResponse completeMPUresponse = client.CompleteMultipartUpload(completeMPUrequest);
            return completeMPUresponse.ArchiveId;
        }

        public static string SetRegion()
        {
            string[] availableRegions = { "us-east-2", "us-east-1", "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-1", "ap-northeast-3", "ap-northeast-2",
            "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3", "eu-north-1",
            "me-south-1", "sa-east-1"};

            string region = "";
            bool invalid = true;
            while (invalid)
            {
                Console.WriteLine("Which Region would you like to upload the archive to (e.g. US-West-2)");
                region = Console.ReadLine();

                if (availableRegions.Contains(region.ToLower()))
                {
                    invalid = false;
                }
                else
                {
                    RedText("Invalid Region");
                    invalid = true;
                }
            }
            return region;
        }

        public static string RedText(string text)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(text);
            Console.ForegroundColor = ConsoleColor.White;

            return text; 
        }
    }
}
