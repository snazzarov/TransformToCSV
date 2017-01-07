using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransformToCSV
{
    public class Transform
    {
        private string connectionString;
        private List<CsvColumns> listTable;
        private CsvColumns csv;
        private List<int> listId;
        string path;
        bool IsFirstTime = true;
        string HeaderWithCaptions = "File, Folder, DuplicateOf, PageType, Patient, RequestID, SiteID, PageFrom, PageTo";
        const string COVER_PAGE = "Cover Page";
        const string EMPTY_STRING = "";

        public Transform(string connDb, string pathFile)
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
            Console.WriteLine(string.Format("Documents selecte to process: {0}", listId.Count));
        }
        public void ExportToCSVFile()
        {
            if (IsFirstTime)
            {
                StreamWriter file1 = new StreamWriter(path);
                file1.WriteLine(HeaderWithCaptions);
                file1.Close();
                IsFirstTime = false;
            }

            string row = EMPTY_STRING;
            using (System.IO.StreamWriter file = File.AppendText(path))
            {
                //new System.IO.StreamWriter(@))
                //                row = String.Join(", ", listTable);
                foreach (CsvColumns csv in listTable)
                {
                    row = csv.FileName + ", " +
                          csv.FolderName + ", " +
                          csv.DuplicatOf + ", " +
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
            foreach (int idd in listId)
            {
//                if (idd == 4)
//                {
                    FillingPageList(idd);
                    ExportToCSVFile();
                    TransformList();
                    ExportToCSVFile();
//                }
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
                prevType = listTable[i - 1].PageType;
                // miss Cover Page 
                if (String.IsNullOrEmpty(prevType))
                    continue;
                // check unequality current and previous PageType
                // check equality current and previous PatientName
                // assign current PageType to previous row
                if ((!prevType.Equals(listTable[i].PageType)) &&
                    (listTable[i].PatientName.Equals(listTable[i - 1].PatientName)))
                {
                    listTable[i - 1].PageType = listTable[i].PageType;
                    // assign previous PageFrom to curlistTablerent row
                    //                    listTable[i].PageFrom = [i - 1].PageFrom;
                    // remove previous row at all
                    //                    listTable.RemoveAt(i - 1);
                    // adjust index var after removing
                    //                    i--;
                }
            }
        }
        private List<string> FindPatients()
        {
            var uniquePatients = listTable
                .Where(x => x.PageType != COVER_PAGE)
                .Select(x => x.PatientName)
                .Distinct()
                .ToList();
            return uniquePatients;
/*
            List<string> resultList = new List<string>();
            string patient = "";
            int i = 0;
            SkipCoverPage(ref i);
            for (; i < listTable.Count; i++)
                if (!IsEqualPatients(patient, listTable[i].PatientName))
                {
                    resultList.Add(listTable[i].PatientName);
                    patient = listTable[i].PatientName;
                }
                else
                    continue;
            return resultList;
*/
        }
        /*
         * condensed : if records are condensed: record contains of several pages: from PageFrom upto PageTo.
         */
        private int NumberOfPages(bool condensed)
        {
            int result = 0;
            for (int i = 0; i < listTable.Count; i++)
                if (condensed)
                    result += listTable[i].PageTo - listTable[i].PageFrom + 1;
                else
                    result += 1;
            return result;
        }
        private void List10Percent(string patient, ref int iFirstProcessed, ref int iLastProcessed)
        {
            int pageNumber = 0;
            for (int i = 0; i < listTable.Count; i++)
                if (IsEqualPatients(patient, listTable[i].PatientName))
                {
                    pageNumber += 1;
                    if (iFirstProcessed == -1)
                        iFirstProcessed = i;
                    iLastProcessed = i;
                }
            // pageNumber += listTable[i].PageTo - listTable[i].PageFrom;
            if ((100 * pageNumber) / NumberOfPages(false) >= 10)
                iFirstProcessed = iLastProcessed = -1;            
            // return iFirstProcessed and iLastProcessed
        }
        private void Assign10Patient(string patient, int iFirstPatient, int iLastPatient)
        {
            for(int i = iFirstPatient; i <= iLastPatient; i++)
                listTable[i].PatientName = patient;
        }
        /*
            3) if patient is found in less than 10% of records -> consider this to be Previous (if no previous -> next)
            Note: if p1 is mentioned in the doc more than once -> keep as is
        */
        private void Check3rdCondition()
        {
//            int iFirstProcessed = -1, iLastProcessed = -1;
            List<Tuple<string,int>> distinctPatientsCounts = listTable
                .Where(x=>x.PageType != COVER_PAGE)
                .GroupBy(x=>x.PatientName)
                .Select(x=>Tuple.Create<string,int>(x.Key, x.Count()))
                .ToList();

            var candidatesToRemove = distinctPatientsCounts
                .Where(x => x.Item2 <= listTable.Count / 10)
                .ToList();

            if (candidatesToRemove.Count != 1)
                return;
            
            int indexOfFirst = listTable
                                .Select((x,index)=>Tuple.Create<string,int>(x.PatientName, index))
                                .Where(x =>x.Item1 == candidatesToRemove[0].Item1)
                                .First()
                                .Item2;

            //int indexOfFirst = listTable.Where(x=>x.PageNr == pageOfFirst).

            int indexOfLast = indexOfFirst + candidatesToRemove[0].Item2 - 1;

            //int iRecordToTake = indexOfFirst == 0 ? indexOfLast + 1 : indexOfFirst - 1;
            int iRecordToTake = FindBasicPage(indexOfFirst, indexOfLast);
            if (iRecordToTake == -1)
                return;
            for (int i=indexOfFirst; i <= indexOfLast; i++)
            {
                listTable[i].PatientName = listTable[iRecordToTake].PatientName;
                listTable[i].SiteId = listTable[iRecordToTake].SiteId;
                listTable[i].ChartId = listTable[iRecordToTake].ChartId;
                listTable[i].PageType = listTable[iRecordToTake].PageType;
            }

            /*
            //            var distinctPatients = FindPatients();
            for (int i = 0; i < distinctPatients.Count; i++)
            {
                if (i == 0)
                {       // 1st patient: take next patient
                    List10Percent(distinctPatients[0], ref iFirstProcessed, ref iLastProcessed);
                    if (distinctPatients.Count > 1 && iFirstProcessed >= 0)
                    {
                        Assign10Patient(distinctPatients[1], iFirstProcessed, iLastProcessed);
                        continue;
                    }
                }
                if (i == distinctPatients.Count - 1)
                {       // last patient: take previous patient
                    iFirstProcessed = iLastProcessed = -1;
                    List10Percent(distinctPatients[i-1], ref iFirstProcessed, ref iLastProcessed);
                    if (iFirstProcessed >= 0 && i > 1)
                        Assign10Patient(distinctPatients[i - 1], iFirstProcessed, iLastProcessed);
                    continue;
                }
                // intermidiate patients: take previous patient
                iFirstProcessed = iLastProcessed = -1;
                List10Percent(distinctPatients[i], ref iFirstProcessed, ref iLastProcessed);
                if (iFirstProcessed >= 0 && i > 1)
                    Assign10Patient(distinctPatients[i - 1], iFirstProcessed, iLastProcessed);
            }
            */
        }

        private int FindBasicPage(int indexOfFirst, int indexOfLast)
        {
            for (int i=indexOfFirst-1; i >= 0; i--)
            {
                if (listTable[i].PageType != COVER_PAGE)
                    return i;
            }
            for (int i=indexOfLast + 1;i<listTable.Count; i++)
            {
                if (listTable[i].PageType != COVER_PAGE)
                    return i;
            }
            return -1;
        }

        private void SkipCoverPage(ref int iCurrent)
    {
        for (; iCurrent < listTable.Count; iCurrent++)
            if (IsEqualPatients(listTable[iCurrent].PageType, COVER_PAGE))
                continue;
            else
                break;
    }
    private List<CsvColumns> GroupPages(int iCurrent, ref int iLastProcessed, ref string patient)
    {
        List<CsvColumns> group = new List<CsvColumns>();
        patient = listTable[iCurrent].PatientName;
        iCurrent++;
        for (; iCurrent < listTable.Count; iCurrent++)
            if (IsEqualPatients(listTable[iCurrent].PatientName, patient))
                group.Add(listTable[iCurrent]);
            else
            {
                iLastProcessed = iCurrent;
                return group;
            }
        iLastProcessed = listTable.Count - 1;
        return group;
    }
    private void AssignPatient(string patient, List<CsvColumns> list)
    {
        for (int i = 0; i < list.Count; i++)
            list[i].PatientName = patient;
    }
    private bool IsEqualPatients(string patient1, string patient2)
    {
        return patient1.ToUpper() == patient2.ToUpper();
    }
    /*
    4) if we have situation for pages:
        1-m pages - Patien1, m+1-N -> Pationt2, N-last Patient1 -> Patient 1
    */
    private void Check4thCondition()
    {
        int iLastProcessed = 0;
        string patient1 = "", patient2 = "", patient3 = "";
        var firstList = new List<CsvColumns>();
        var secondList = new List<CsvColumns>();
        var thirdList = new List<CsvColumns>();
        int i = 0;
        SkipCoverPage(ref i);
        firstList = GroupPages(i, ref iLastProcessed, ref patient1);
        if (firstList.Count > 0)
        {
            i = iLastProcessed + 1;
            patient2 = patient1;
            secondList = GroupPages(i, ref iLastProcessed, ref patient2);
            if ((secondList.Count > 0) &&
                 !IsEqualPatients(patient1, patient2))
            {
                i = iLastProcessed + 1;
                thirdList = GroupPages(i, ref iLastProcessed, ref patient3);
                if ((thirdList.Count > 0) &&
                     IsEqualPatients(patient3, patient1) &&
                    !IsEqualPatients(patient3, patient2))
                {
                    AssignPatient(patient1, secondList);
                    listTable.Clear();
                    listTable.AddRange(firstList);
                    listTable.AddRange(secondList);
                    listTable.AddRange(thirdList);
                }
            }
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
        /*
         * 5) cover page records for the document, if go one-after-another should be condesed to 1 with page from..to
         */
        public void Check5thCondition()
        {
            int prevPage = -1, prevIndex = -1, firstPage = -1;
            for (int i = 0; i < listTable.Count(); i++)
            {
                if (listTable[i].PageType.Equals(COVER_PAGE))
                {
                    // first iteration 
                    if (prevPage == -1)
                    {
                        prevPage = listTable[i].PageFrom;
                        firstPage = listTable[i].PageFrom;
                        prevIndex = i;
                        continue;
                    }
                    if (((prevIndex + 1) == i) &&
                        ((prevPage + 1) == listTable[i].PageFrom))
                    {
                        // mark for removing previous item
                        listTable[prevIndex].PageFrom = -1;
                        listTable[prevIndex].PageTo = -1;
                        prevPage++;
                        prevIndex++;
                        continue;
                    }
                    else        // not adjacent pages 
                    {
                        // close range
                        // update last record
                        listTable[prevIndex].PageFrom = firstPage;
                        listTable[prevIndex].PageTo = prevPage;
                        // init vars
                        firstPage = listTable[i].PageFrom;
                        prevPage = firstPage;
                        prevIndex = i;
                        continue;
                    }
                }
            }
            if (listTable.Count() > 0)
                if (listTable[listTable.Count - 1].PageType.Equals(COVER_PAGE))
                {
                    listTable[listTable.Count - 1].PageFrom = firstPage;
                }
            // remove all marked items
            for (int i = 0; i < listTable.Count() - 1; i++)
                if (listTable[i].PageFrom == -1)
                {
                    listTable.RemoveAt(i);
                    i--;
                }
        }
        private string GetChartId(int startPage, int endPage)
        {
            var groups = listTable
                        .Where(w => ((w.PageFrom == -1) && (w.PageTo == -1)) || (w.PageFrom == startPage) && (w.PageTo == endPage))
                        .GroupBy(n => n.ChartId)
                        .Select(n => new
                        {
                            chart_id = n.Key,
                            chart_count = n.Count()
                        })
                        .OrderByDescending(n => n.chart_count);
            if (groups.ToList().Count() > 0)
                return groups.ToList()[0].chart_id;
            return EMPTY_STRING;
        }
        private int GetSiteId(int startPage, int endPage, string chartId)
        {
            var groups = listTable
                        .Where(w => (((w.PageFrom == -1) && (w.PageTo == -1) ||
                              (w.PageFrom == startPage && (w.PageTo == endPage))) && (w.ChartId == chartId)))
                        .GroupBy(n => n.SiteId)
                        .Select(n => new
                        {
                            site_id = n.Key,
                            site_count = n.Count()
                        })
                        .OrderByDescending(n => n.site_count);
            if (groups.ToList().Count() > 0)
                return groups.ToList()[0].site_id;
            return -1;
        }
        private string GetPageType(int startPage, int endPage, string chartId)
        {
            var groups = listTable
                        .Where(w => (((w.PageFrom == -1) && (w.PageTo == -1) ||
                              (w.PageFrom == startPage && (w.PageTo == endPage))) && (w.ChartId == chartId)))
                        .GroupBy(n => n.PageType)
                        .Select(n => new
                        {
                            page_type = n.Key,
                            page_type_count = n.Count()
                        })
                        .OrderByDescending(n => n.page_type_count);
            if (groups.ToList().Count() > 0)
                return groups.ToList()[0].page_type;
            return EMPTY_STRING;
        }
        private void Check56Condition()
        {
            var resultList = new List<CsvColumns>();
            for (int i = 0; i < listTable.Count; i++)
            {
                if (listTable[i].PageType.Equals(COVER_PAGE))
                {
                    resultList.Add(listTable[i]);
                    int iLastProcessed = i;
                    CondensedCoverPage(i, ref iLastProcessed);
                    i = iLastProcessed;
                }
                else
                {
                    resultList.Add(listTable[i]);
                    int iLastProcessed = i;
                    CondensedPatient(i, ref iLastProcessed);
                    i = iLastProcessed;
                }
            }
            listTable = resultList;
        }
        private void CondensedPatient(int iCurrent, ref int iLastProcessed)
        {
            int i = iCurrent;
            var pg = listTable[iCurrent];
            for (i = iCurrent + 1; i < listTable.Count; i++)
                if (!pg.isTheSamePatient(listTable[i]))
                {
                    pg.PageTo = listTable[i - 1].PageTo;
                    iLastProcessed = i - 1;
                    return;
                }
            pg.PageTo = listTable[listTable.Count - 1].PageFrom;
            iLastProcessed = listTable.Count - 1;
        }
        /*
* 6) if patient is the same on several pages -> one record, now we often have several, example i
*/
        private void Check6thCondition()
        {
            int prevPage = -1, prevIndex = -1, firstPage = -1;
            //, siteId = -1;
            string patient = EMPTY_STRING;
            //, chartId = "", pageType = "";

            for (int i = 0; i < listTable.Count(); i++)
            {
                if (!listTable[i].PageType.Equals(COVER_PAGE))
                {
                    int iLastProcessed = i;
                    CondensedCoverPage(i, ref iLastProcessed);
                    i = iLastProcessed;
                    // first iteration 
                    if (prevPage == -1)
                    {
                        patient = listTable[i].PatientName;
                        prevPage = listTable[i].PageFrom;
                        firstPage = listTable[i].PageFrom;
                        prevIndex = i;
                        continue;
                    }
                    if (((prevIndex + 1) == i) &&
                        ((prevPage + 1) == listTable[i].PageFrom) &&
                        (patient == listTable[i].PatientName))
                    {
                        // mark for removing previous item
                        listTable[prevIndex].PageFrom = -1;
                        listTable[prevIndex].PageTo = -1;
                        prevPage++;
                        prevIndex++;
                        continue;
                    }
                    else        // not adjacent pages or changed patient
                    {
                        // close range
                        // update last record
                        listTable[prevIndex].PageFrom = firstPage;
                        listTable[prevIndex].PageTo = prevPage;
                        listTable[prevIndex].ChartId = GetChartId(firstPage, prevPage);
                        listTable[prevIndex].SiteId = GetSiteId(firstPage, prevPage, listTable[prevIndex].ChartId);
                        listTable[prevIndex].PageType = GetPageType(firstPage, prevPage, listTable[prevIndex].ChartId);
                        // init vars
                        firstPage = listTable[i].PageFrom;
                        prevPage = firstPage;
                        prevIndex = i;
                        patient = listTable[i].PatientName;
                        continue;
                    }
                }
            }
            if (listTable.Count() > 0)
                if (listTable[listTable.Count - 1].PageType.Equals(COVER_PAGE))
                {
                    listTable[listTable.Count - 1].PageFrom = firstPage;
                }
            // remove all marked items
            for (int i = 0; i < listTable.Count() - 1; i++)
                if (listTable[i].PageFrom == -1)
                {
                    listTable.RemoveAt(i);
                    i--;
                }
        }
        private void CondensedCoverPage(int iCurrent, ref int iLastProcessed)
        {
            int i = iCurrent;
            var pg = listTable[iCurrent];
            for (i = iCurrent + 1; i < listTable.Count; i++)
            {
                if (!pg.isCoverPage(listTable[i]))
                {
                    pg.PageTo = listTable[i - 1].PageTo;
                    iLastProcessed = i - 1;
                    return;
                }
            }
            pg.PageTo = listTable[listTable.Count - 1].PageFrom;
            iLastProcessed = listTable.Count - 1;
        }
        private void TransformList()
        {
            Check2ndCondition();
            //            Check4thCondition();
            //            Check5thCondition();
            //            Check6thCondition();
            Check3rdCondition();
          //  Check4thCondition();
          //  Check56Condition();
            //            Check3rdCondition();

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
@"Select  distinct
		d.ImageFiles as FileName, 
        d.OriginalName as FolderName, 
		d.DuplicateOf as DuplicatOf,
	    t.Name as PageType, 
		'' as PatientName,
		null as ChartId, 
		p.PreSiteID as SiteId,
		p.PageNr as PageNr,
        p.PageNr as PageFrom,
     	p.PageNr as PageTo
From [Documents] d inner join [DocumentPages] p 
		on (d.ID = p.DocumentID)
		inner join [DocumentPageTypes] t
		on (t.ID = p.PreDocumentPageTypeID) 
		and t.Name in ('Cover Page')
		and d.Id = 3
union 
Select  distinct 
		d.ImageFiles as FileName, 
        d.OriginalName as FolderName, 
		d.DuplicateOf as DuplicatOf,
	    t.Name as PageType, 
		p.PreMemberFirstName + ' ' + p.PreMemberLastName as PatientName,
		p.PreChartID as ChartId, 
		p.PreSiteID as SiteId,
		p.PageNr as PageNr,
		p.PageNr as pageFrom,
		p.PageNr as PageTo
From [Documents] d inner join [DocumentPages] p 
		on (d.ID = p.DocumentID)
		inner join DocumentPageTypes t
		on (t.ID = p.PreDocumentPageTypeID) 
		and t.Name not in ('Poor Quality', 'Cover Page')
		and d.Id = 3
Order by PageFrom";
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.Parameters.AddWithValue("DocId", idd);
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        CsvColumns csv = new CsvColumns();
                        csv.FileName = rdr[0] as string;
                        csv.FolderName = rdr[1] as string;
                        csv.DuplicatOf = (int)((rdr[2] as int?) ?? 0);
                        csv.PageType = rdr[3] as string;
                        csv.PatientName = (rdr[4] as string) ?? string.Empty; // bug can be null later used as not null
                        csv.ChartId = rdr[5] as string ?? string.Empty;
                        csv.SiteId = (int)((rdr[6] as int?) ?? 0);
                        csv.PageNr = (int)rdr[7];
                        csv.PageFrom = (int)rdr[8];
                        csv.PageTo = (int)rdr[9];
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
            string outMsg = EMPTY_STRING;
            foreach (CsvColumns e in listTable)
            {
                outMsg = outMsg + ", " + e.FileName + ", " + e.PageNr.ToString() + "\n";
            }
            if (outMsg.Length > 0)
                Console.WriteLine(outMsg);
            else
                Console.WriteLine("Empty");
        }
    }
}
