using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Microsoft.CognitiveSearch.Skills.Image
{
    public class ImageStore
    {
        public CloudBlobContainer libraryContainer;

        public ImageStore(string blobConnectionString, string containerName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(blobConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            libraryContainer = blobClient.GetContainerReference(containerName);
        }

        public async Task<string> UploadImageToLibrary(Stream stream, string name, bool overwrite = false, string contentType= "image/jpg")
        {
            CloudBlockBlob blockBlob = libraryContainer.GetBlockBlobReference(name);
            if (!await blockBlob.ExistsAsync())
            {
                await blockBlob.UploadFromStreamAsync(stream);

                blockBlob.Properties.ContentType = contentType;
                await blockBlob.SetPropertiesAsync();
            }

            return blockBlob.Uri.ToString();
        }

        public Task<string> UploadToBlob(byte[] data, string name, bool overwrite = false)
        {
            return UploadImageToLibrary(new MemoryStream(data), name, overwrite);
        }

        public Task<string> UploadToBlob(string data, string name, bool overwrite = false)
        {
            return UploadImageToLibrary(new MemoryStream(Convert.FromBase64String(data)), name, overwrite);
        }
    }
    public class AnnotationStore : ImageStore
    {
        public AnnotationStore(string blobConnectionString, string containerName) : base(blobConnectionString, containerName)
        {


        }
        //public async Task<string> SaveAnnotation(Stream stream, string name, bool overwrite = false, string contentType = "image/jpg")
        //{
        //    return await UploadImageToLibrary(stream, name, false, "text/json");
        //}
        public async Task<string> SaveAnnotation( string data, string name, RunInfo runInfo, bool overwrite = false, string contentType = "image/jpg")
        {
            name = string.Format("{0}/{1}", runInfo.toString(), name);
            
            using (var stream = GenerateStreamFromString(data))
            {
                return await UploadImageToLibrary(stream, name, false, "text/json");
            }
                
        }
        public static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

    }

    public class RunInfo
    {
        public string corpus { get; set; }
        public string document { get; set; }
        public string skill { get; set; }
        public DateTime runInstance { get; set; }
        public int seqId { get; set; }

        public string toString()
        {
            return string.Format("{0}/{1}/{2}/{3}", runInstance.ToString("MMM-dd-yyyy").ToLower(),corpus,document,skill);
        }
    }

}
