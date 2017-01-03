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
        public string PageType;
        public string PatientName;
        public string ChartId;
        public int SiteId;
        public int PageFrom;
        public int PageTo;
        // PageNr is excluded from CSV-file
        public int PageNr;
    }
    public class CsvProcs { 
        private string connectionString;
//        "Data Source=(localdb)\mssqllocaldb;Initial Catalog=HomeBank;Integrated Security=SSPI;Connection Timeout=180"
        private List<CsvColumns> listTable;
        private CsvColumns csv;
        private List<int> listId;
//        string path = @"E:\Win7\Scripts\DocPages.csv";
        string path;
        public CsvProcs(string connDb, string pathFile)
        {
            connectionString = connDb;
            path = pathFile;
        }
        public void GetDocumentId()
        {
            int currentId;
            listId = new List<int>();
            using (var conn = new SqlConnection(connectionString))
                try
                {
                    conn.Open();
                    string sql = @"Select Id 
                                    From Documents
                                    Order by Id";
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    //                    cmd.Parameters.AddWithValue("DocId", Dob);
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        currentId = (int)rdr[0];
                        listId.Add(currentId);
                    }
                    rdr.Close();
                }
                catch (Exception ex)
                {
                    String message = String.Format("GetDocumentId(): failed to load Ids", ex.Message);
                    Console.WriteLine(message);
                    return;
                }
                finally
                {
                    conn.Close();
                }
                string outMsg = "";
                foreach(int e in listId)
                {
                    outMsg = outMsg + ", " + e.ToString();
                }
                Console.WriteLine(outMsg);
        }

        bool IsFirstTime = true;
        string HeaderWithCaptions = "File, Folder, PageType, Patient, RequestID, SiteID, PageFrom, PageTo";

        public void ExportToCSVFile()
        {
            if (IsFirstTime)
            {
                StreamWriter file1 = new StreamWriter(path);
                file1.WriteLine(HeaderWithCaptions);
                file1.Close();
                IsFirstTime = false;
            }

            string row = "";
            using (System.IO.StreamWriter file = File.AppendText(path))
            {
                //new System.IO.StreamWriter(@))
                //                row = String.Join(", ", listTable);
                foreach (CsvColumns csv in listTable)
                {
                    row = csv.FileName + ", " +
                          csv.FolderName + ", " +
                          csv.PageType + ", " +
                          csv.PatientName + ", " +
                          csv.ChartId + ", " +
                          csv.SiteId.ToString() + ", " +
                          csv.PageFrom.ToString() + ", " +
                          csv.PageTo.ToString();
                    file.WriteLine(row);
                }
                file.Close();
            }
        }
        public void TransformData()
        {
            foreach(int idd in listId)
            {
                FillingPageList(idd);
                TransformList();
                ExportToCSVFile();
            }
        }
        /*
         2) if pages of different types -> last
            don't check Cover Page and Poor Quality
            Pg = Page Number, PT = Page Type
            Pg PT Patient
            1p PQ 
            2p t1 p1
            3p t2 p1
            4p tx p2
            for page 2p: PT=t2
         */
        private void Check2ndCondition()
        {
            string prevType = null;
            for (int i = listTable.Count - 1; i > 0; i--)
            {
                prevType = listTable[i-1].PageType;
                if (prevType.Equals(""))
                    continue; 
                if ((!prevType.Equals(listTable[i].PageType)) &&
                    (listTable[i].PatientName.Equals(listTable[i - 1].PatientName)))
                    listTable[i-1].PageType = listTable[i].PageType;
            }
        }
        private bool Less10Percent(int minI, int maxI)
        {
            if (((maxI - minI + 1) * listTable.Count / 100) < 10)
                return true;
            return false;
        }
        private bool NoPatientFuther(string patient, int i)
        {
            for(int j = i; j<listTable.Count;j++)
                if (listTable[j].PatientName.Equals(patient))
                    return false;
            return true;
        }
        private bool TakeSmallPiece(ref int minI, ref int maxI, ref string prevPatient, ref string nextPatient)
        {
            string patient = "";
            int ii = minI < 0 ? 0 : minI;
            for (int i = ii; i < listTable.Count; i++)
            {
                // these pages can be at the start page: Cover Page, ...
                if (string.IsNullOrEmpty(listTable[i].PatientName)) //.Equals("")) bug!
                    continue;
                if (minI < 0)
                {
                    minI = i;
                    patient = listTable[i].PatientName;
                    prevPatient = "";
                    continue;
                }
                if ((minI == i) && (i > 0))
                    prevPatient = listTable[i - 1].PatientName;
                if (listTable[i].PatientName.Equals(patient))
                    continue;
                else
                {
                    if (Less10Percent(minI, i - 1)) // && NoPatientFuther(patient, i))
                    {
                        maxI = i - 1;
                        if (minI <= maxI)
                        {
                            nextPatient = listTable[i].PatientName;
                            return true;
                        }
                        else
                            return false;
                    }
                }
            }
            return false;
        }
        private void AssignPatient(int minI, int maxI, string patient)
        {
            for (int i = minI; i <= maxI; i++)
                listTable[i].PatientName = patient;
        }
        /*
            3) if patient is found in less than 10% of records -> consider this to be Previous (if no previous -> next)
            Note: if p1 is mentioned in the doc more than once -> keep as is
        */
        private void Check3rdCondition()
        {
            bool result = true;
            int minI = -1, maxI = -1;
            string prevPatient = "", nextPatient = "";
            while (minI < listTable.Count)
            {
                result = TakeSmallPiece(ref minI, ref maxI, ref prevPatient, ref nextPatient);
                if (!result)
                    break;
                if (prevPatient.Length > 0)
                    AssignPatient(minI, maxI, prevPatient);
                else
                    AssignPatient(minI, maxI, nextPatient);
                minI = maxI + 1;
            }
        }
        private string Take1stPiece(ref int minI, ref int maxI)
        {
            string patient = "";
            int j = minI < 0 ? 0 : minI;
            for (int i = j; i < listTable.Count; i++)
            {
                if ((minI == -1) &&
                     !string.IsNullOrEmpty(listTable[i].PatientName))    // .Equals("") bug PatientName can be null   // CP and PQ
                    minI = i;
                if (i == minI)
                {
                    patient = listTable[i].PatientName;
                    continue;
                }
                else
                {
                    if (listTable[i].PatientName.Equals(patient))
                        continue;
                    else
                    {
                        maxI = i - 1;
                        return patient;
                    }
                }
            }
            return "";
        }
        private string Take2ndPiece(ref int minI, ref int maxI, string patent1)
        {
            string patient = "";
            for (int i = minI; i < listTable.Count; i++)
            {
                if (i == minI)
                {
                    patient = listTable[i].PatientName;
                    continue;
                }
                else
                {
                    if (listTable[i].PatientName.Equals(patient))
                        continue;
                    else
                    {
                        maxI = i - 1;
                        if (maxI >= minI)
                            return patient;
                        else
                            return "";
                    }
                }
            }
            return "";
        }
        private bool Take3rdPiece(ref int minI, ref int maxI, string patent1)
        {
            for (int i = minI; i < listTable.Count; i++)
                if (listTable[i].PatientName.Equals(patent1))
                    continue;
                else
                {
                    maxI = i - 1;
                    if (maxI >= minI)
                        return true;
                    return false;
                }
            maxI = listTable.Count - 1;
            return true;
        }
        /*
        4) if we have situation for pages:
            1-m pages - Patien1, m+1-N -> Pationt2, N-last Patient1 -> Patient 1
        */
        private void Check4thCondition()
        {
            bool result = true;
            int min1 = -1, max1 = -1, min2 = -1, max2 = -1, min3 = -1, max3 = -1;
            string patient1 = "", patient2 = "";
            while (min1 < listTable.Count)
            {
                patient1 = Take1stPiece(ref min1, ref max1);
                if (patient1.Length == 0)
                    break;
                min2 = max1 + 1;
                patient2 = Take2ndPiece(ref min2, ref max2, patient1);
                if (patient2.Length == 0)
                    break;
                min3 = max2 + 1;
                result = Take3rdPiece(ref min3, ref max3, patient1);
                if (!result)
                {
                    min1 = max3 + 1;
                    continue;
                }
                AssignPatient(min2, max2, patient1);
                min1 = max3 + 1;
            }
        }
        private void AssignPages(int from, int to)
        {
            for (int i = from; i <= to; i++)
            {
                listTable[i].PageFrom = listTable[from].PageNr;
                listTable[i].PageTo = listTable[to].PageNr;
            }
        }
        private void Boundaries()
        {
            string patient = "";
            int from = -1, to = -1;
            for (int i = 0; i < listTable.Count; i++)
            {
                if (listTable[i].PatientName.Equals(""))
                    continue;
                if (from == -1)
                {
                    from = i;
                    patient = listTable[i].PatientName;
                }
                if (!listTable[i].PatientName.Equals(patient))
                {
                    to = i - 1;
                    if (from <= to)
                    {
                        AssignPages(from, to);
                        from = i;
                    }
                }
            }
            AssignPages(from, listTable.Count - 1);   
        }
        private void TransformList()
        {
            Check2ndCondition();
            Check3rdCondition();
            Check4thCondition();
//            Boundaries();
        }
        private void FillingPageList(int idd)
        {
            listTable = new List<CsvColumns>();
            csv = new CsvColumns();
            using (var conn = new SqlConnection(connectionString))
                try
                {
                    conn.Open();
                    string sql =
@"select *
from (
Select  d.ImageFiles as FileName, 
        d.OriginalName as FolderName, 
	    t.Name as PageType, 
		'' as PatientName,
		null as ChartId, 
		p.PreSiteID as SiteId,
       p.PageNr as PageFrom
     	,p.PageNr as PageTo
From [VercendPOC_Staging].[dbo].[Documents] d inner join [VercendPOC_Staging].[dbo].[DocumentPages] p 
		on (d.ID = p.DocumentID)
		inner join [VercendPOC_Staging].[dbo].[DocumentPageTypes] t
		on (t.ID = p.PreDocumentPageTypeID) 
		and t.Name in ('Cover Page')
		and d.Id = @DocId
--group by d.ImageFiles, d.OriginalName, t.Name, p.PreSiteID
union 
Select  d.ImageFiles as FileName, 
        d.OriginalName as FolderName, 
	    t.Name as PageType, 
		p.PreMemberFirstName + ' ' + p.PreMemberLastName as PatientName,
		p.PreChartID as ChartId, 
		p.PreSiteID as SiteId
		,min(p.PageNr) as pageFrom
		,max(p.PageNr) as PageTo
From [VercendPOC_Staging].[dbo].[Documents] d inner join [VercendPOC_Staging].[dbo].[DocumentPages] p 
		on (d.ID = p.DocumentID)
		inner join [VercendPOC_Staging].[dbo].[DocumentPageTypes] t
		on (t.ID = p.PreDocumentPageTypeID) 
		and t.Name not in ('Poor Quality', 'Cover Page')
		and d.Id = @DocId
group by d.ImageFiles, d.OriginalName, t.Name, p.PreMemberFirstName + ' ' + p.PreMemberLastName, p.PreChartID, p.PreSiteID
) tbl
Order by PageFrom";
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.Parameters.AddWithValue("DocId", idd);
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        //                        csv.Id = (int)rdr[0];
                        CsvColumns csv = new CsvColumns();
                        csv.FileName = rdr[0] as string;
                        csv.FolderName = rdr[1] as string;
                        csv.PageType = rdr[2] as string;
                        csv.PatientName = (rdr[3] as string) ?? string.Empty; // bug can be null later used as not null
                        csv.ChartId = rdr[4] as string ?? string.Empty; 
                        csv.SiteId = (int)((rdr[5] as int?) ?? 0);
                        csv.PageFrom = (int)rdr[6];
                        csv.PageTo = (int)rdr[7];
//                        csv.PageNr = (int)rdr[6];
                        listTable.Add(csv);
                    }
                    rdr.Close();
                }
                catch (Exception ex)
                {
                    String message = String.Format("FillingList: failed list filling",
                        ex.Message);
                    Console.WriteLine(message + " " + ex.Message);
                    return;
                }
                finally
                {
                    conn.Close();
                }
                string outMsg = "";
                foreach (CsvColumns e in listTable)
                {
                    outMsg = outMsg + ", " + e.FileName + ", " + e.PageNr.ToString() + "\n\r";
                }
            if (outMsg.Length > 0)
                Console.WriteLine(outMsg);
            else
                Console.WriteLine("Empty");
        }
    }
}
