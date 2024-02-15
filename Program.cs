using CsvHelper;
using Dapper;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Globalization;
using System.Text;

var localFilePath = ConfigurationManager.AppSettings["LocalPath"];
var filePrepend = ConfigurationManager.AppSettings["FilePrepend"];
var dayInterval = int.Parse(ConfigurationManager.AppSettings["DayInterval"] ?? "14");
var excludeFuturusData = Convert.ToBoolean(ConfigurationManager.AppSettings["ExcludeFuturusData"]);
var dateFormat = "yyyyMMdd";
var fullLocalFilePath = $"{localFilePath}/{filePrepend}_{DateTime.Now.ToString(dateFormat)}";

var ExcludedFuturusData = /*excludeFuturusData*/false ? "AND us.IsFuturus != 1" : string.Empty; // TODO: Remove false and uncomment excludeFuturusData when implemented

var _getAllSessionData = @$"
    SELECT
	    es.EmployeeId,
        es.AppVersionNumber,
	    sqr.SessionId,
        q.QuizId,
	    q.Name as QuizName,
        sqr.EmployeeAnswer,
        sqr.Score,
        sqr.Attempt,
        sqr.TimeStampUtc AS TimeStamp
    FROM sessionquizresult sqr
	    JOIN quiz q ON sqr.QuizId = q.QuizId
        JOIN employeesession es ON es.SessionId = sqr.SessionId
    WHERE es.StartTimeUtc >= DATE(NOW() - INTERVAL @DaysInThePast DAY) {ExcludedFuturusData}
    ORDER BY TimeStampUtc ASC;";

var connectionString = "server=delta-deicingvr.mysql.database.azure.com;uid=deltaadmin;pwd=FuturFile!5;database=dbo";
using var connection = new MySqlConnection(connectionString);

FileInfo fileInfo = new FileInfo(fullLocalFilePath);
if (!fileInfo.Exists)
{
    Directory.CreateDirectory(fullLocalFilePath);
}

var allSessionDataParameters = new DynamicParameters();
allSessionDataParameters.Add("@DaysInThePast", dayInterval);
var allSessionData = connection.Query<SessionData>(_getAllSessionData, allSessionDataParameters);
using (var writer = new StreamWriter($"{fullLocalFilePath}/{filePrepend}_{DateTime.Now.ToString(dateFormat)}_ALL.csv", false, Encoding.UTF8))
{
    var csv = new CsvWriter(writer, CultureInfo.CurrentCulture);
    csv.WriteRecords(allSessionData);
    csv.Flush();
}

class SessionData
{
    public string EmployeeId { get; set; }
    public string AppVersionNumber { get; set; }
    public string SessionId { get; set; }
    public string QuizId { get; set; }
    public string QuizName { get; set; }
    public string EmployeeAnswer { get; set; }
    public string Score { get; set; }
    public string Attempt { get; set; }
    public string TimeStamp { get; set; }
}