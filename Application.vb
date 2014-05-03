Imports System.Data.SqlClient
Imports System.Data.Sql
Imports System
Imports System.Data
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Configuration

Module Application

    Dim SQLConnection As SqlConnection

    Sub Main()
        Dim connectionString As String = ConfigurationManager.ConnectionStrings("gemConnectionString").ConnectionString
        SQLConnection = New SqlConnection(connectionString)
        SQLConnection.Open()

        ParseLog()

        process("Construct")
        process("Measure")
        SQLConnection.Close()
    End Sub
    Sub ParseLog()
        'this is the folder where are the logs are currently stored
        Dim myDocPath As String = ConfigurationManager.AppSettings("log_location")

        'This is the table where the read filenames are stored to avoid repeating entries in the database
        Dim FileTable As String = "rReadFiles"

        'this is the table in the database where the Logs that we will use for group creation will be stored
        Dim LogStorageTable As String = "rImportedLogs"
        '#############################################################################
        Dim path As String = Directory.GetCurrentDirectory()
        Dim Botpattern As String = "(bot)|(spider)|(slurp)|(fdm)|(curl)|(synapse)|(crawl)"
        Dim CIdPattern As String = "(cid=)(\d*)"
        Dim MIdPattern As String = "(mid=)(\d*)"
        Dim Access_date, time, visitorIP As String
        Dim itemID = vbNullString
        Dim IDmatch As MatchCollection
        Dim Access_time As DateTime
        Dim CIDExists, MIDExists As Boolean
        Dim RecordsAffected As Integer
        Dim CurrentLine As String
        Dim currentRow As String()
        Dim logData As StreamReader


        'Step one'
        'We check all the log files out of a specified directory. if there are multiple files we will go through them in croniological order
        '  addendum this has only been checked with files that end nameYYMM.log where name is the same on all files YY are the last 2 digits of the year and
        '           MM is the two digit representation of the month
        'changes to the log naming schema will result in this needing to be checked again to ensure that logs are read in cronological order
        Dim dataTable = New DataTable
        Dim sqlCommand
        For Each logfile As String In Directory.EnumerateFiles(myDocPath, "*.log")
            Dim last_edit = FileDateTime(logfile)
            Dim current_last_edit As DateTime = DateTime.MinValue
            Using dataAdapter = New SqlDataAdapter("SELECT last_edit FROM " + FileTable + " WHERE fileName='" + logfile + "'", SQLConnection)
                ' you dont need to open/close the connection with a DataAdapter '
                dataAdapter.Fill(dataTable)
            End Using
            If dataTable.Rows.Count = 0 Then
                ' new file
                sqlCommand = New SqlCommand("Insert into " + FileTable + " (filename,last_edit) values('" + logfile + "', '" + last_edit + "')", SQLConnection)
                RecordsAffected = sqlCommand.ExecuteNonQuery()

            Else
                current_last_edit = dataTable.Rows(0).Item(0).ToString
                If current_last_edit = last_edit Then
                    Continue For
                Else
                    ' existing file
                    sqlCommand = New SqlCommand("UPDATE " + FileTable + " Set last_edit='" + last_edit + "' WHERE fileName='" + logfile + "'", SQLConnection)
                    RecordsAffected = sqlCommand.ExecuteNonQuery()

                End If
            End If

            dataTable.Clear()

            'if it is a new log we store its name in the database currently full path because if the database structure changes and the files need to be reread 
            'we can place all old logs in their new location and not have to worry about false positives in the database for a full reread
            'Now we are ready to scrape the log
            logData = New StreamReader(logfile, FileMode.Open)

            While (logData.Peek <> -1)
                Try
                    CIDExists = False
                    MIDExists = False
                    CurrentLine = logData.ReadLine()
                    'the logs are space delimited so we have to split each individual line on spaces
                    currentRow = CurrentLine.Split(New Char() {" "c})
                    'we need to ensure that each line has at least 12 partitions or it will not have the information we need
                    '  to determine if it should go into the database for the next step
                    If currentRow.Length > 11 Then
                        'the bot pattern contains a list of words that you find in queries that are just trawling the website and
                        '    have a negative impact on our results
                        If Regex.IsMatch(currentRow(12), Botpattern, RegexOptions.IgnoreCase) Then
                            Continue While
                        End If
                        'checking for a cid
                        If Regex.IsMatch(currentRow(7), CIdPattern) Then
                            IDmatch = Regex.Matches(currentRow(7), CIdPattern)
                            CIDExists = True
                            If IDmatch.Item(0).ToString = "cid=" Then
                                CIDExists = False
                            End If
                            itemID = IDmatch.Item(0).ToString
                        End If
                        'checking for a mid'
                        If Regex.IsMatch(currentRow(7), MIdPattern) Then
                            IDmatch = Regex.Matches(currentRow(7), MIdPattern)
                            MIDExists = True
                            If IDmatch.Item(0).ToString = "mid=" Then
                                MIDExists = False
                            End If
                            itemID = IDmatch.Item(0).ToString
                        End If
                        If Not CIDExists And Not MIDExists Then
                            Continue While
                        End If
                        Access_date = currentRow(0)
                        time = currentRow(1)
                        visitorIP = currentRow(10)
                        Access_time = Convert.ToDateTime(Access_date + " " + time)
                        If Access_time < current_last_edit Then
                            Continue While
                        End If
                    Else
                        Continue While
                    End If
                    'once we have pulled all th information we need from the logfile and we have tedermined it is one we want to youse we insert it into the database
                    Dim parts As String() = itemID.Split((New Char() {"="}))
                    Dim CorM As Char
                    If CIDExists Then
                        CorM = "c"
                    ElseIf MIDExists Then
                        CorM = "m"
                    End If
                    If CorM = "c" Or CorM = "m" Then
                        sqlCommand = New SqlCommand("INSERT INTO " + LogStorageTable + "([dateTimeAccessed],[IpAddress],[itemTYPE],[itemID]) Values('" + Access_time + "','" + visitorIP + "', '" + CorM + "','" + parts(1) + "') ", SQLConnection)
                        sqlCommand.ExecuteNonQuery()
                    End If

                Catch ex As Microsoft.VisualBasic.FileIO.MalformedLineException
                    Console.WriteLine("Line " & ex.Message & "is not valid and will be skipped.")
                End Try
            End While
        Next
    End Sub
    Function Entropy(ByRef Numbers As Array) As Double
        Dim Sum = 0
        For i = 0 To Numbers.Length - 1
            Sum = Numbers(i) + Sum
        Next
        Dim result = 0
        For i = 0 To Numbers.Length - 1
            Dim X As Integer = Numbers(i)
            If X <= 0 Then Continue For
            result = result + X * Math.Log(X / Sum)
        Next
        Return -result
    End Function

    Sub process(ByVal Model As String)
        '#########################SET UP VARIBLES###############################
        'like in the log digester step your this database connection string will be differnt for you

        Dim months_to_look_back As Integer = ConfigurationManager.AppSettings("months_to_look_back")

        'this is the table where you can look up a mapping of the particular group id you are using to find which MID group it belongs too
        'Dim MIDtoGroupTable As String = ""

        'This is the same table we stored the logs in in the fullLogDigester program, now we need to read them all
        Dim LogStorageTable As String = "rImportedLogs"

        'this is the table where you will store your results for the CIDs and under the current design the MIDs
        '#######################################################################
        Dim row As DataRow
        Dim IpToGroup As New Dictionary(Of String, Integer)
        Dim GroupToTime As New Dictionary(Of String, DateTime)
        Dim GroupToVisited As New Dictionary(Of Integer, LinkedList(Of Integer))
        Dim CIDGroupToVisitCount As New Dictionary(Of Integer, Integer)
        Dim CIDToCount As New Dictionary(Of Integer, Integer)
        Dim CIDGroup_count As Integer = 0
        Dim InfoTable = New DataTable
        Dim sqlCommand As New SqlCommand
        Dim closestMatches As New LinkedList(Of Tuple)
        Dim results = Tuple.Create(0.0, "Null")
        Dim columnEntropy As Double
        Dim rowEntropy As Double
        Dim totalEntropy As Double
        Dim LRR As Double
        Dim resultsCount As Integer = 0
        Dim Count1, Count2 As Integer

        CIDGroup_count = 0

        '##########LOG TABLE CALL##############################
        Using da = New SqlDataAdapter("SELECT dateTimeAccessed, IpAddress, itemID FROM " & LogStorageTable & " where itemTYPE = '" & Model(0) & "' and dateTimeAccessed > '" & DateAdd(DateInterval.Month, -months_to_look_back, DateTime.Now) & "'", SQLConnection)
            da.Fill(InfoTable)
        End Using

        For Each row In InfoTable.Rows
            'this is the check if the row should be sorted according to cid objects or mid objects
            Dim _IpAddress = row.Item("IpAddress")
            Dim _item As Integer = row.Item("itemID")
            Dim within_timeframe As Boolean
            Dim _IpToGroup As Integer = -1
            If IpToGroup.ContainsKey(_IpAddress) Then
                _IpToGroup = IpToGroup.Item(_IpAddress)
                within_timeframe = DateDiff(DateInterval.Day, GroupToTime.Item(_IpToGroup), row.Item("dateTimeAccessed")) <= 10080
            End If
            'this checks if the IpAddress has already been assigned to a group Id
            If _IpToGroup <> -1 Then
                'if exists check time to see if it is in the window to be a new group or appended to the current one
                If within_timeframe Then
                    GroupToTime.Item(_IpToGroup) = row.Item("dateTimeAccessed")
                    CIDGroupToVisitCount.Item(_IpToGroup) += 1
                    GroupToVisited.Item(_IpToGroup).AddLast(_item)
                Else
                    'Map this visit to a new group if it is outside the time window
                    _IpToGroup = IpToGroup.Item(_IpAddress) = CIDGroup_count
                    GroupToVisited.Add(CIDGroup_count, New LinkedList(Of Integer))
                    GroupToVisited(CIDGroup_count).AddLast(_item)
                    CIDGroupToVisitCount.Add(CIDGroup_count, 1)
                    GroupToTime.Add(CIDGroup_count, row.Item("dateTimeAccessed"))
                    CIDGroup_count += 1
                End If
            Else
                'make new group as necessary
                IpToGroup.Add(_IpAddress, CIDGroup_count)
                GroupToVisited.Add(CIDGroup_count, New LinkedList(Of Integer))
                GroupToVisited(CIDGroup_count).AddLast(_item)
                CIDGroupToVisitCount.Add(CIDGroup_count, 1)
                GroupToTime.Add(CIDGroup_count, row.Item("dateTimeAccessed"))
                CIDGroup_count += 1
            End If
            If Not CIDToCount.ContainsKey(_item) Then
                CIDToCount.Add(_item, 0)
            End If
        Next
        'freeing up no longer needed memory
        GroupToTime.Clear()
        IpToGroup.Clear()
        InfoTable.Clear()
        Dim removableKeys = (From pair In CIDGroupToVisitCount Where pair.Value < 5 Select pair.Key).ToArray
        For Each key In removableKeys
            CIDGroupToVisitCount.Remove(key)
            GroupToVisited.Remove(key)
            CIDGroup_count -= 1
        Next
        CIDGroup_count += CIDGroupToVisitCount.Count
        Dim ResultsArray(CIDToCount.Count()) As Tuple(Of Double, Integer, Boolean)
        Dim PoolIDs As New Dictionary(Of Integer, Integer)
        For Each key2 In CIDToCount.Keys.ToList
            CIDToCount.Item(key2) += GroupToVisited.Where(Function(obj) obj.Value.Contains(key2)).Count
        Next
        Using da = New SqlDataAdapter("SELECT " & Model & "Id as ObjID, PoolId FROM t" & Model & " left join rcategories cat on constructid = cat.variableid", SQLConnection)
            da.Fill(InfoTable)
        End Using
        For Each row In InfoTable.Rows
            If Not IsNothing(row.Item("PoolId")) Then
                PoolIDs.Add(row.Item("ObjID"), row.Item("PoolId"))
            End If
        Next
        InfoTable.Clear()
        'SZ TODO: Make this section a little more functionally borken up to make it easier to repeat for MIDs
        For Each key1 In CIDToCount.Keys()
            resultsCount = 0
            Dim PoolID = -1
            If PoolIDs.ContainsKey(key1) Then
                PoolID = PoolIDs(key1)
            End If
            For Each key2 In CIDToCount.Keys()
                'get values for algorythm
                If key1 <> key2 Then
                    Dim Overlap_Count = GroupToVisited.Where(Function(obj) obj.Value.Contains(key1) And obj.Value.Contains(key2)).Count
                    Dim FirstKeyOnly = CIDToCount.Item(key1) - Overlap_Count
                    Dim SecondKeyOnly = CIDToCount.Item(key2) - Overlap_Count
                    Dim NeitherKey = CIDGroup_count - Count1 - Count2 + Overlap_Count
                    
                    rowEntropy = Entropy({Overlap_Count, SecondKeyOnly}) + Entropy({FirstKeyOnly, NeitherKey})
                    columnEntropy = Entropy({Overlap_Count, FirstKeyOnly}) + Entropy({SecondKeyOnly, NeitherKey})
                    totalEntropy = Entropy({FirstKeyOnly, SecondKeyOnly, Overlap_Count, NeitherKey})
                    LRR = 2 * (totalEntropy - rowEntropy - columnEntropy)
                    If (Overlap_Count > 0) Then
                        Dim _PoolID = -1
                        If PoolIDs.ContainsKey(key2) Then
                            _PoolID = PoolIDs(key2)
                        End If
                        ResultsArray(resultsCount) = Tuple.Create(LRR, key2, PoolID = _PoolID)
                        resultsCount += 1
                    End If
                End If
            Next
            Array.Sort(ResultsArray)
            Array.Reverse(ResultsArray)
            ' we will be submitting the top ten values in the list generated above
            ' if there is we need to overwrite that row
            Dim StorageTable = "r" & Model & "Sim"
            sqlCommand = New SqlCommand("Delete From " & StorageTable & " Where FocalKey='" & key1 & "'", SQLConnection)
            sqlCommand.ExecuteNonQuery()
            Dim sCounter = 1
            Dim rCounter = 1
            For counter As Integer = 0 To 99
                If IsNothing(ResultsArray(counter)) Then
                ElseIf ResultsArray(counter).Item1 <> -1 Then
                    Dim relationship
                    Dim _counter
                    If ResultsArray(counter).Item3 Then
                        If sCounter > 10 Then
                            Continue For
                        End If
                        _counter = sCounter
                        sCounter += 1
                        relationship = "s"
                    Else
                        If rCounter > 10 Then
                            Continue For
                        End If
                        _counter = rCounter
                        rCounter += 1
                        relationship = "r"
                    End If
                    sqlCommand = New SqlCommand("insert into " & StorageTable & "(FocalKey, RelationshipType, RecommendationNo, RelatedId, Score) values(" & key1 & ", '" & relationship & "', " & _counter & ", " & ResultsArray(counter).Item2 & ", " & ResultsArray(counter).Item1 & ")", SQLConnection)
                    sqlCommand.ExecuteNonQuery()
                End If
            Next
        Next
    End Sub

End Module
