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
        Dim pagePattern As Regex = New Regex("/Public/((Measure)|(Construct))Detail.aspx", RegexOptions.IgnoreCase)
        Dim Botpattern As Regex = New Regex("(bot)|(spider)|(slurp)|(fdm)|(curl)|(synapse)|(crawl)", RegexOptions.IgnoreCase)
        Dim CIdPattern As Regex = New Regex("(cid=)(\d*)", RegexOptions.IgnoreCase)
        Dim MIdPattern As Regex = New Regex("(mid=)(\d*)", RegexOptions.IgnoreCase)
        Dim Access_date, time, visitorIP As String
        Dim itemID = vbNullString
        Dim Access_time As DateTime
        Dim CIDExists, MIDExists As Boolean
        Dim RecordsAffected As Integer
        Dim logData As StreamReader


        'Step one'
        'We check all the log files out of a specified directory. if there are multiple files we will go through them in croniological order
        '  addendum this has only been checked with files that end nameYYMM.log where name is the same on all files YY are the last 2 digits of the year and
        '           MM is the two digit representation of the month
        'changes to the log naming schema will result in this needing to be checked again to ensure that logs are read in cronological order
        Dim dataTable = New DataTable
        Dim sqlCommand
        For Each logfile As String In Directory.EnumerateFiles(myDocPath, "*.log")
            Dim current_last_line = ""
            Using dataAdapter = New SqlDataAdapter("SELECT last_line FROM " + FileTable + " WHERE fileName='" + logfile + "'", SQLConnection)
                ' you dont need to open/close the connection with a DataAdapter '
                dataAdapter.Fill(dataTable)
            End Using
            Dim is_new_file = True
            If dataTable.Rows.Count > 0 Then
                current_last_line = dataTable.Rows(0).Item(0).ToString
                is_new_file = False
            End If

            dataTable.Clear()

            'if it is a new log we store its name in the database currently full path because if the database structure changes and the files need to be reread 
            'we can place all old logs in their new location and not have to worry about false positives in the database for a full reread
            'Now we are ready to scrape the log
            logData = New StreamReader(logfile, FileMode.Open)
            Dim CurrentLine = ""
            While (logData.Peek <> -1)
                Try
                    CIDExists = False
                    MIDExists = False
                    Dim PageID As String = ""
                    CurrentLine = logData.ReadLine()
                    If current_last_line <> "" Then
                        If CurrentLine = current_last_line Then
                            current_last_line = ""
                        End If
                        Continue While
                    End If
                    'the logs are space delimited so we have to split each individual line on spaces
                    Dim currentRow = CurrentLine.Split(New Char() {" "c})
                    'we need to ensure that each line has at least 12 partitions or it will not have the information we need
                    '  to determine if it should go into the database for the next step
                    If currentRow.Length = 22 Then
                        'the bot pattern contains a list of words that you find in queries that are just trawling the website and
                        '    have a negative impact on our results
                        If Botpattern.IsMatch(currentRow(12)) Then
                            Continue While
                        End If
                        'checking for a cid
                        Dim PM As Match = pagePattern.Match(currentRow(6))
                        If PM.Success Then
                            Dim CorM As String = "x"
                            Dim IDmatch As Match = Nothing
                            If PM.Groups(1).Value.ToUpper() = "CONSTRUCT" Then
                                CIDExists = True
                                IDmatch = CIdPattern.Match(currentRow(7))
                                CorM = "c"
                            End If
                            If PM.Groups(1).Value.ToUpper() = "MEASURE" Then
                                MIDExists = True
                                IDmatch = MIdPattern.Match(currentRow(7))
                                CorM = "m"
                            End If
                            If IDmatch.Success Then
                                PageID = IDmatch.Groups(2).Value
                                If PageID <> "" Then
                                    Access_date = currentRow(0)
                                    time = currentRow(1)
                                    visitorIP = currentRow(10)
                                    Access_time = Convert.ToDateTime(Access_date + " " + time)

                                    'once we have pulled all th information we need from the logfile and we have tedermined it is one we want to youse we insert it into the database

                                    sqlCommand = New SqlCommand("INSERT INTO " + LogStorageTable + "([dateTimeAccessed],[IpAddress],[itemTYPE],[itemID]) Values('" + Access_time + "','" + visitorIP + "', '" + CorM + "','" + PageID + "') ", SQLConnection)
                                    sqlCommand.ExecuteNonQuery()
                                End If
                            End If
                        End If
                    End If
                Catch ex As Microsoft.VisualBasic.FileIO.MalformedLineException
                    Console.WriteLine("Line " & ex.Message & "is not valid and will be skipped.")
                End Try
            End While

            If is_new_file Then
                ' new file
                sqlCommand = New SqlCommand("Insert into " + FileTable + " (filename,last_line) values('" + logfile + "', '" + CurrentLine + "')", SQLConnection)
                RecordsAffected = sqlCommand.ExecuteNonQuery()

            Else
                sqlCommand = New SqlCommand("UPDATE " + FileTable + " Set last_line='" + CurrentLine + "' WHERE fileName='" + logfile + "'", SQLConnection)
                RecordsAffected = sqlCommand.ExecuteNonQuery()
            End If
        Next
    End Sub
    Function Entropy(ByRef Numbers As Array) As Double
        Dim Sum As Double = 0
        For i = 0 To Numbers.Length - 1
            Sum = Numbers(i) + Sum
        Next
        Dim result = 0
        For i = 0 To Numbers.Length - 1
            Dim X As Integer = Numbers(i)
            If X <= 0 Then Continue For
            result = result + X * Math.Log(X)
        Next
        Return Sum * Math.Log(Sum) - result
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
        Dim IPSession As New Dictionary(Of String, Integer)
        Dim SessionTime As New Dictionary(Of String, DateTime)
        Dim Session As New Dictionary(Of Integer, HashSet(Of Integer))
        Dim ItemCount As New Dictionary(Of Integer, Integer)
        Dim IndexCounter As Integer = 0
        Dim InfoTable = New DataTable
        Dim sqlCommand As New SqlCommand
        Dim results = Tuple.Create(0.0, "Null")
        Dim columnEntropy As Double
        Dim rowEntropy As Double
        Dim totalEntropy As Double
        Dim resultsCount As Integer = 0
        '##########LOG TABLE CALL##############################
        Using da = New SqlDataAdapter("SELECT dateTimeAccessed, IpAddress, itemID FROM " & LogStorageTable & " where itemTYPE = '" & Model(0) & "' and dateTimeAccessed > '" & DateAdd(DateInterval.Month, -months_to_look_back, DateTime.Now) & "' order by dateTimeAccessed asc", SQLConnection)
            da.Fill(InfoTable)
        End Using

        For Each row In InfoTable.Rows
            'this is the check if the row should be sorted according to cid objects or mid objects
            Dim _IpAddress As String = row.Item("IpAddress")
            Dim itemID As Integer = row.Item("itemID")
            Dim index As Integer = -1
            If IPSession.ContainsKey(_IpAddress) Then
                index = IPSession.Item(_IpAddress)
                'if exists check time to see if it is in the window to be a new group or appended to the current one
                If DateDiff(DateInterval.Minute, SessionTime.Item(index), row.Item("dateTimeAccessed")) <= 10080 Then
                    SessionTime.Item(index) = row.Item("dateTimeAccessed")
                    Session.Item(index).Add(itemID)
                Else
                    'Map this visit to a new group if it is outside the time window
                    IPSession.Item(_IpAddress) = IndexCounter
                    Session.Add(IndexCounter, New HashSet(Of Integer)({itemID}))
                    SessionTime.Add(IndexCounter, row.Item("dateTimeAccessed"))
                    IndexCounter += 1
                End If
            Else
                'make new group as necessary
                IPSession.Add(_IpAddress, IndexCounter)
                Session.Add(IndexCounter, New HashSet(Of Integer)({itemID}))
                SessionTime.Add(IndexCounter, row.Item("dateTimeAccessed"))
                IndexCounter += 1
            End If
        Next
        'freeing up no longer needed memory
        SessionTime.Clear()
        IPSession.Clear()
        InfoTable.Clear()

        Dim removableKeys = (From pair In Session Where pair.Value.Count < 6 Select pair.Key).ToArray
        For Each key In removableKeys
            Session.Remove(key)
        Next

        Dim SessionTable = "rSessions"
        sqlCommand = New SqlCommand("Delete From " & SessionTable & " where model='" & Model(0) & "'", SQLConnection)
        sqlCommand.ExecuteNonQuery()
        For Each key As Integer In Session.Keys
            For Each value In Session(key)
                sqlCommand = New SqlCommand("insert into " & SessionTable & " values ( " & key + 1 & ", " & value & ", '" & Model(0) & "')", SQLConnection)
                sqlCommand.ExecuteNonQuery()
            Next
        Next

        Dim PoolIDs As New Dictionary(Of Integer, Integer)
        For Each key In Session.Keys
            Dim items = Session(key)
            For Each value In items
                If ItemCount.ContainsKey(value) Then
                    ItemCount.Item(value) += 1
                Else
                    ItemCount.Add(value, 1)
                End If
            Next
        Next
        Dim ResultsArray(ItemCount.Count()) As Tuple(Of Double, Integer, Boolean)
        Using da = New SqlDataAdapter("SELECT " & Model & "Id as ObjID, PoolId FROM t" & Model & " left join rcategories cat on constructid = cat.variableid", SQLConnection)
            da.Fill(InfoTable)
        End Using
        For Each row In InfoTable.Rows
            If Not IsNothing(row.Item("PoolId")) And Not IsDBNull(row.Item("PoolId")) Then
                PoolIDs.Add(row.Item("ObjID"), row.Item("PoolId"))
            End If
        Next
        InfoTable.Clear()
        For Each key1 In ItemCount.Keys()
            resultsCount = 0
            Dim PoolID = -1
            If PoolIDs.ContainsKey(key1) Then
                PoolID = PoolIDs(key1)
            End If
            For Each key2 In ItemCount.Keys()
                'get values for algorythm
                If key1 <> key2 Then
                    Dim Overlap_Count = Session.Where(Function(obj) obj.Value.Contains(key1) And obj.Value.Contains(key2)).Count
                    Dim FirstKeyOnly = Session.Where(Function(obj) obj.Value.Contains(key1) And Not obj.Value.Contains(key2)).Count
                    Dim SecondKeyOnly = Session.Where(Function(obj) Not obj.Value.Contains(key1) And obj.Value.Contains(key2)).Count
                    Dim NeitherKey = Session.Where(Function(obj) Not obj.Value.Contains(key1) And Not obj.Value.Contains(key2)).Count

                    rowEntropy = Entropy({Overlap_Count + SecondKeyOnly, FirstKeyOnly + NeitherKey})
                    columnEntropy = Entropy({Overlap_Count + FirstKeyOnly, SecondKeyOnly + NeitherKey})
                    totalEntropy = Entropy({FirstKeyOnly, SecondKeyOnly, Overlap_Count, NeitherKey})
                    If rowEntropy + columnEntropy > totalEntropy Then
                        Dim LRR As Double = 2 * (rowEntropy + columnEntropy - totalEntropy)
                        Dim _PoolID = -1
                        If PoolIDs.ContainsKey(key2) Then
                            _PoolID = PoolIDs(key2)
                        End If
                        Dim Scaled_LRR = 1 - 1 / (1 + LRR)
                        ResultsArray(resultsCount) = Tuple.Create(Scaled_LRR, key2, PoolID = _PoolID)
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
            For counter As Integer = 0 To ResultsArray.Length - 1
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
