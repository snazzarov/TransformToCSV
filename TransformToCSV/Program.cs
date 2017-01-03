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
            //"Data Source = (localdb)\\MSSQLLocalDB; Initial Catalog = VercendPOC_Staging; Integrated Security = SSPI";
            string filePath = Properties.Settings.Default.OutputFilePath;

            CsvProcs prc = new CsvProcs(connection, filePath);
            prc.GetDocumentId();
            prc.TransformData();
            //prc.ExportToCSVFile();
        }
    }
}
