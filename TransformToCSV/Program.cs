using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransformToCSV
{
    class Program
    {
        static void Main(string[] args)
        {
            string connection = Properties.Settings.Default.ConnectionString;
            string filePath = Properties.Settings.Default.OutputFilePath;
            string logFilePath = Properties.Settings.Default.LogFilePath;
            Transform prc = new Transform(connection, filePath, logFilePath);
            prc.GetDocumentId();
            prc.TransformData();
        }
    }
}
