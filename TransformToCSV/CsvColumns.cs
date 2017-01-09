using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransformToCSV
{
    public class CsvColumns
    {
        public int Id;
        public string FileName;
        public string FolderName;
        public int DuplicatOf;
        public string PageType;
        public string PatientName;
        public string ChartId;
        public int SiteId;
        public int PageFrom;
        public int PageTo;
        // PageNr is excluded from CSV-file
        public int PageNr;

        internal bool isTheSamePatient(CsvColumns csvColumns)
        {
            return PatientName.ToUpper() == csvColumns.PatientName.ToUpper();
        }
        internal bool isCoverPage(CsvColumns csvColumns)
        {
            return String.IsNullOrEmpty(csvColumns.PatientName);
        }
    }
}
