using System.Collections.Generic;
using System.Text.Json;
using System;
using System.IO;

namespace SBWSFinanceApi.Logger.DAC
{
    internal sealed class LoggerDac
    {

        private string path = @"C:\POC\log\log.txt";
        private void SetCompletePath()
        {
            /*idea is to look for a location and there create rolling files*/
            // var files = Directory.GetFiles()
        }
        internal List<Log> Get()
        {
            List<Log> deserializedResult = null;
            if (System.IO.File.Exists(path))
            {
                // check the file size
                // System.IO.DirectoryInfo(
                //Read the content of the JSON DB
                var fileContents = System.IO.File.ReadAllText(path);
                //Serialize the JSON value            
                deserializedResult = JsonSerializer.Deserialize<List<Log>>(fileContents);
            }


            return deserializedResult;
        }

        internal void Save(List<Log> logs)
        {
            // Create a Serializer Object
            // var serializer = new JsonSerializer();
            //Serialize the list content
            var serializedResult = JsonSerializer.Serialize(logs);

            try
            {
                // check if folder exists 
                if (!Directory.Exists(@"C:\POC\log"))
                {
                    Directory.CreateDirectory(@"C:\POC\log");
                }
                // if (!File.Exists(path))
                // {
                //     File.Create(path);
                // }
                //Save the result
                System.IO.File.WriteAllText(path, serializedResult);
            }
            catch (Exception ex)
            { }
        }
    }
}