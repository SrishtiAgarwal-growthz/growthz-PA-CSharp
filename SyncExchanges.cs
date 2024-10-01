using IntegrationLibrary;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Configuration;
using System;
using System.Text.Json;
using Portal.Data.Models;
using Portal.Data.Repository;
using Portal.Domain;

namespace SyncExchanges
{
    public class SyncExchanges
    {
        private readonly ILogger<SyncExchanges> _logger;
        private readonly IConfiguration _configuration;
        private readonly ICampaignResultRepository _campaignResultRepository;
        private readonly ICampaignRepository _campaignRepository;
        private readonly IExchangeRepository _exchangeRepository;
        private string _connString = string.Empty;
        private const string _outbrainBaseURL = "https://api.outbrain.com/amplify/v0.1";

        private readonly int _numberOfDaysToSync;
        private const string _mgidAPIBaseURL = "https://api.mgid.com/v1";
        private const int _mgidExchangeID = 3;
        private int _outbrainExchangeId = 2;
        private const string _taboolaAPIBaseURL = "https://backstage.taboola.com/backstage/api/1.0";
        private const int _taboolaExchangeId = 4;  // Assign the appropriate exchange ID for Taboola



        public SyncExchanges(ILogger<SyncExchanges> logger, IConfiguration configuration, ICampaignResultRepository campaignResultRepository,
            IExchangeRepository exchangeRepository,
            ICampaignRepository campaignRepository)
        {
            _logger = logger;
            this._configuration = configuration;
            this._campaignResultRepository = campaignResultRepository;
            this._campaignRepository = campaignRepository;
            this._exchangeRepository = exchangeRepository;
            _numberOfDaysToSync = Convert.ToInt32(Environment.GetEnvironmentVariable("OutbrainDaysToSync") ?? "3");
        }

        [Function("SyncOutbrianUsingAPI")]
        public async Task<IActionResult> Run([TimerTrigger("0 */27 * * * *")] TimerInfo myTimer)
        //public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _connString = Environment.GetEnvironmentVariable("DefaultConnection") ?? "";
            _outbrainExchangeId = Convert.ToInt32(Environment.GetEnvironmentVariable("OutbrainExchangeID") ?? "2");

            IList<CampaignsToSync> list = new List<CampaignsToSync>();
            var campaign = await _exchangeRepository.GetCampaignToProcess();
            if (campaign == null)
            {
                var fullList = await GetCampaignsForOutbrainAsync();
                foreach (var c in fullList)
                {
                    SyncQueue s = new SyncQueue();
                    s.ClientId = c.ClientId;
                    s.ExchangeId = c.ExchangeId;
                    s.CampaignId = c.CampaignId;
                    s.ExchangeCampaignId = c.ExchangeCampaignId;
                    s.IsWaiting = true;
                    s.QueueEnterTime = DateTime.Now;
                    await _exchangeRepository.AddSyncQueue(s);
                }
                list.Add(fullList.First());
            }
            else
            {
                //Convert campaign object & add new object in list
                CampaignsToSync s = new CampaignsToSync();
                s.ClientId = campaign.ClientId;
                s.ExchangeId = campaign.ExchangeId;
                s.CampaignId = campaign.CampaignId;
                s.ExchangeCampaignId = campaign.ExchangeCampaignId;
                s.DataFromDate = DateOnly.FromDateTime(DateTime.Now.AddDays(_numberOfDaysToSync * -1));
                s.DataToDate = DateOnly.FromDateTime(DateTime.Now);
                //s.DataFromDate = new DateOnly(2024, 5, 15);
                //s.DataToDate = new DateOnly(2024, 6, 15);
                list.Add(s);
            }
            //await fetchMGIDData(list, _connString);
            if (list.Any(c => c.ExchangeId == _outbrainExchangeId))
            {
                await FetchOutbrainData(list.Where(c => c.ExchangeId == _outbrainExchangeId).ToList(), _connString);
            }

            if (list.Any(c => c.ExchangeId == _mgidExchangeID))
            {
                await FetchMgidData(list.Where(c => c.ExchangeId == _mgidExchangeID).ToList(), _connString);
            }

            if (list.Any(c => c.ExchangeId == _taboolaExchangeId))
            {
                await FetchTaboolaData(list.Where(c => c.ExchangeId == _taboolaExchangeId).ToList(), _connString);
            }


            foreach (var c in list)
            {
                await _exchangeRepository.RemoveSyncQueue(c.ClientId, c.CampaignId, c.ExchangeId, c.ExchangeCampaignId);
            }

            _logger.LogInformation(@"C# HTTP trigger function processed a request.");

            return new OkResult();
        }

        private async Task FetchMgidData(IList<CampaignsToSync> campsi, string connString)
        {
            var token = await GetMgidToken(_mgidAPIBaseURL);
            var mgid = new MGID(token, _mgidAPIBaseURL);
        }

        private async Task FetchOutbrainData(IList<CampaignsToSync> campaignsToSync, string connString)
        {
            var token = await GetOutbrainToken(_outbrainBaseURL);
            var outbrain = new Outbrain(token, _outbrainBaseURL);

            foreach (var c in campaignsToSync)
            {
                var campaignResult = await outbrain.GetCampaignPerformanceResult(c.ExchangeCampaignId, c.DataFromDate.ToString("yyyy-MM-dd"), c.DataToDate.ToString("yyyy-MM-dd"));
                await StoreCampaignData(campaignResult, c, c.DataFromDate, "date");

                for (DateOnly d = c.DataToDate; d >= c.DataFromDate; d = d.AddDays(-1))
                {
                    var result = await outbrain.GetPerformanceByCountry(c.ExchangeCampaignId, d.ToString("yyyy-MM-dd"), d.ToString("yyyy-MM-dd"));
                    await StoreCampaignData(result, c, d, "country");
                }
                for (DateOnly d = c.DataToDate; d >= c.DataFromDate; d = d.AddDays(-1))
                {
                    var result = await outbrain.GetPerformanceByOS(c.ExchangeCampaignId, d.ToString("yyyy-MM-dd"), d.ToString("yyyy-MM-dd"));
                    await StoreCampaignData(result, c, d, "os");
                }
                for (DateOnly d = c.DataToDate; d >= c.DataFromDate; d = d.AddDays(-1))
                {
                    var result = await outbrain.GetPerformanceByBrowser(c.ExchangeCampaignId, d.ToString("yyyy-MM-dd"), d.ToString("yyyy-MM-dd"));
                    await StoreCampaignData(result, c, d, "browser");
                }
                for (DateOnly d = c.DataToDate; d >= c.DataFromDate; d = d.AddDays(-1))
                {
                    var result = await outbrain.GetPerformanceByRegion(c.ExchangeCampaignId, d.ToString("yyyy-MM-dd"), d.ToString("yyyy-MM-dd"));
                    await StoreCampaignData(result, c, d, "region");
                }
                for (DateOnly d = c.DataToDate; d >= c.DataFromDate; d = d.AddDays(-1))
                {
                    var result = await outbrain.GetPerformanceByDomain(c.ExchangeCampaignId, d.ToString("yyyy-MM-dd"), d.ToString("yyyy-MM-dd"));
                    await StoreCampaignData(result, c, d, "domain");
                }
                for (DateOnly d = c.DataToDate; d >= c.DataFromDate; d = d.AddDays(-1))
                {
                    var result = await outbrain.GetPerformanceByAds(c.ExchangeCampaignId, d.ToString("yyyy-MM-dd"), d.ToString("yyyy-MM-dd"));
                    await StoreCampaignData(result, c, d, "ads");
                }
            }
        }

        private async Task FetchTaboolaData(IList<CampaignsToSync> campaignsToSync, string connString)
        {
            // Get the Taboola access token
            var token = await GetTaboolaToken(_taboolaAPIBaseURL);
            var taboola = new TaboolaApiClient(token);

            foreach (var campaign in campaignsToSync)
            {
                // Replace with your Taboola-specific API calls
                var campaignSummary = await taboola.GetCampaignSummaryReportAsync(
                    campaign.ExchangeCampaignId.ToString(),
                    campaign.DataFromDate.ToString("yyyy-MM-dd"),
                    campaign.DataToDate.ToString("yyyy-MM-dd"),
                    "campaign",
                    "impressions,clicks,spend"
                );

                // Process and store the campaign summary data
                await StoreCampaignData(campaignSummary, campaign, campaign.DataFromDate, "summary");
            }
        }


        private async Task StoreCampaignData(string result, CampaignsToSync c, DateOnly dateOfData, string resultByParam = "")
        {

            var fullObject = new CampaignResponseObject();
            fullObject = JsonSerializer.Deserialize<CampaignResponseObject>(result);
            fullObject.CampaignDetails = c;
            switch (resultByParam)
            {
                case "date":
                default:
                    await _campaignResultRepository.UpsertCampaignMetric(fullObject);
                    await _campaignResultRepository.UpdateConsolidatedCampaignData(fullObject);
                    break;
                case "country":
                    await _campaignResultRepository.UpsertCampaignMetricByCountry(fullObject, dateOfData);
                    break;
                case "os":
                    await _campaignResultRepository.UpsertCampaignMetricByOS(fullObject, dateOfData);
                    break;
                case "browser":
                    await _campaignResultRepository.UpsertCampaignMetricByBrowser(fullObject, dateOfData);
                    break;
                case "region":
                    await _campaignResultRepository.UpsertCampaignMetricByRegions(fullObject, dateOfData);
                    break;
                case "domain":
                    await _campaignResultRepository.UpsertCampaignMetricByDomains(fullObject, dateOfData);
                    break;
                case "ads":
                    await _campaignResultRepository.UpsertCampaignMetricByAds(fullObject, dateOfData);
                    await _campaignResultRepository.UpsertConsolidatedAdsData(fullObject);
                    break;
            }
            await _exchangeRepository.AddSyncLog(c, resultByParam, dateOfData);
        }

        private async Task<string> GetOutbrainToken(string outbrainBaseURL)
        {
            string tokenStr = string.Empty;

            var obToken = await _exchangeRepository.GetAPIToken(_outbrainExchangeId);
            if (obToken == null || obToken.TokenGenerationDate.AddHours(6) < DateTime.Now)
            {
                var outbrainLib = new Outbrain("", _outbrainBaseURL);
                tokenStr = await outbrainLib.GetAPIToken("api@performacemedia.com", "Abc12345#");
                tokenStr = ParseOutbrainJsonToGetToken(tokenStr);
                await _exchangeRepository.UpdateAPIToken(_outbrainExchangeId, tokenStr);
            }
            else
                tokenStr = obToken.Token;

            return tokenStr;
        }

        private async Task<string> GetMgidToken(string mgidBaseURL)
        {
            string tokenStr = string.Empty;

            var mgidToken = await _exchangeRepository.GetAPIToken(_mgidExchangeID);
            if (mgidToken == null || mgidToken.TokenGenerationDate.AddHours(6) < DateTime.Now)
            {
                var mgidLib = new MGID("", _mgidAPIBaseURL);
                tokenStr = await mgidLib.GetAPIToken("api@performacemedia.com", "Abc12345#", mgidBaseURL);
                tokenStr = ParseMgidJsonToGetToken(tokenStr);
                await _exchangeRepository.UpdateAPIToken(_mgidExchangeID, tokenStr);
            }
            else
                tokenStr = mgidToken.Token;

            return tokenStr;
        }

        private async Task<string> GetTaboolaToken(string taboolaBaseURL)
        {
            string tokenStr = string.Empty;

            var taboolaToken = await _exchangeRepository.GetAPIToken(_taboolaExchangeId);
            if (taboolaToken == null || taboolaToken.TokenGenerationDate.AddHours(6) < DateTime.Now)
            {
                // Implement your logic for fetching the Taboola token if it is expired or not present
                var taboolaLib = new TaboolaApiClient("");  // Create Taboola client with empty token
                tokenStr = await taboolaLib.GetAccessToken("api@performacemedia.com", "TaboolaPassword#");

                // Store the new token
                await _exchangeRepository.UpdateAPIToken(_taboolaExchangeId, tokenStr);
            }
            else
                tokenStr = taboolaToken.Token;

            return tokenStr;
        }


        private string ParseMgidJsonToGetToken(string tokenStr)
        {
            string result = string.Empty;
            // Deserialize the JSON token string
            var jsonDocument = JsonDocument.Parse(tokenStr);

            // Read the value of the "OB-TOKEN-V1" parameter
            if (jsonDocument.RootElement.TryGetProperty("token", out var tokenValue))
            {
                result = tokenValue.GetString();
            }
            return result;
        }

        private string ParseOutbrainJsonToGetToken(string tokenStr)
        {
            string result = string.Empty;
            // Deserialize the JSON token string
            var jsonDocument = JsonDocument.Parse(tokenStr);

            // Read the value of the "OB-TOKEN-V1" parameter
            if (jsonDocument.RootElement.TryGetProperty("OB-TOKEN-V1", out var tokenValue))
            {
                result = tokenValue.GetString();
            }

            return result;
        }

        private async Task<IList<CampaignsToSync>> GetCampaignsForOutbrainAsync()
        {
            var list = new List<CampaignsToSync>();
            var exchangeCampaigns = await _campaignRepository.GetActiveExchangeCampaigns(_outbrainExchangeId);
            foreach (var c in exchangeCampaigns.OrderByDescending(y => y.CampaignId))
            {
                CampaignsToSync s = new CampaignsToSync();
                s.ClientId = c.Campaign.CustomerId;
                s.ExchangeId = _outbrainExchangeId;
                s.CampaignId = c.CampaignId;
                s.ExchangeCampaignId = c.ExchangeCampaignId;
                s.DataFromDate = DateOnly.FromDateTime(DateTime.Now.AddDays(_numberOfDaysToSync * -1));
                s.DataToDate = DateOnly.FromDateTime(DateTime.Now);
                list.Add(s);
            }
            return list;
        }
    }
}
