using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Portal.Data.Models;
using Portal.Domain;
using Portal.Domain.DTO;
using Portal.Domain.VM;
using System.Diagnostics;
using Microsoft.AspNetCore.Mvc.Diagnostics;

namespace Portal.Data.Repository
{
    public class CampaignResultRepository : BaseRepository, ICampaignResultRepository
    {
        private readonly PortalDbContext _context;

        public CampaignResultRepository(PortalDbContext context) : base(context)
        {
            _context = this.context;
        }

        #region Upsert Campaign Metrics methods
        public async Task UpsertCampaignMetric(CampaignResponseObject campaignResponseObject)
        {
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetrics
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == DateOnly.Parse(result.metadata.fromDate)
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetric
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        DataForDate = DateOnly.Parse(result.metadata.fromDate),
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetrics.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;

                    _context.CampaignMetrics.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }

        public async Task UpsertTaboolaCampaignMetric(CampaignResponseObject campaignResponseObject)
        {
            if (campaignResponseObject?.results == null)
                return;

            foreach (var result in campaignResponseObject.results)
            {
                // Check if a record already exists for CampaignId, ExchangeId, and DataForDate
                var existingMetrics = await _context.CampaignMetrics
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                                && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                                && x.DataForDate == DateOnly.Parse(result.metadata.fromDate)
                                && x.IsActive == true).ToListAsync();

                if (existingMetrics.Count == 0)
                {
                    // Create a new CampaignMetric record
                    var newMetric = new CampaignMetric
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        DataForDate = DateOnly.Parse(result.metadata.fromDate),
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Admin",
                        CreatedById = 1,
                        CreatedOn = DateTime.UtcNow
                    };

                    await _context.CampaignMetrics.AddAsync(newMetric);
                }
                else
                {
                    // Update the existing CampaignMetric record
                    var existingMetric = existingMetrics.FirstOrDefault();
                    existingMetric.Impressions = result.metrics.impressions;
                    existingMetric.Clicks = result.metrics.clicks;
                    existingMetric.Ctr = result.metrics.ctr;
                    existingMetric.Spend = result.metrics.spend;
                    existingMetric.Ecpc = result.metrics.ecpc;
                    existingMetric.TotalConversions = result.metrics.totalConversions;
                    existingMetric.Conversions = result.metrics.conversions;
                    existingMetric.ViewConversions = result.metrics.viewConversions;
                    existingMetric.Cpa = result.metrics.cpa;
                    existingMetric.TotalCpa = result.metrics.totalCpa;
                    existingMetric.TotalSumValue = result.metrics.totalSumValue;
                    existingMetric.SumValue = result.metrics.sumValue;
                    existingMetric.ViewSumValue = result.metrics.viewSumValue;
                    existingMetric.TotalAverageValue = result.metrics.totalAverageValue;
                    existingMetric.AverageValue = result.metrics.averageValue;
                    existingMetric.ViewAverageValue = result.metrics.viewAverageValue;
                    existingMetric.Roas = result.metrics.roas;
                    existingMetric.TotalRoas = result.metrics.totalRoas;
                    existingMetric.IsActive = true;
                    existingMetric.LastModifiedBy = "Admin";
                    existingMetric.LastModifiedById = 1;
                    existingMetric.LastModifiedOn = DateTime.UtcNow;

                    _context.CampaignMetrics.Update(existingMetric);
                }

                await _context.SaveChangesAsync();
            }
        }

        public async Task UpsertCampaignMetricByCountry(CampaignResponseObject campaignResponseObject, DateOnly dateOfData)
        {
            if (campaignResponseObject.results == null)
                return;
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetricByCountries
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == dateOfData
                        && x.CountryCode.ToUpper() == result.metadata.code.ToUpper()
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetricByCountry
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        Country = result.metadata.name,
                        CountryCode = result.metadata.code,
                        ExchangeCountry = result.metadata.name,
                        ExchangeCountryCode = result.metadata.code,
                        DataForDate = dateOfData,
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetricByCountries.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;
                    campaignMetricExisting.Country = result.metadata.name;
                    campaignMetricExisting.CountryCode = result.metadata.code;
                    campaignMetricExisting.ExchangeCountry = result.metadata.name;
                    campaignMetricExisting.ExchangeCountryCode = result.metadata.code;

                    _context.CampaignMetricByCountries.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpsertCampaignMetricByOS(CampaignResponseObject campaignResponseObject, DateOnly dateOfData)
        {
            if (campaignResponseObject.results == null)
                return;
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetricByOS
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == dateOfData
                        && x.Platform.ToUpper() == result.metadata.platform.ToUpper()
                        && x.OS.ToUpper() == result.metadata.name.ToUpper()
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetricByOS
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        OS = result.metadata.name,
                        Platform = result.metadata.platform,
                        ExchangeOSId = result.metadata.name,
                        ExchangePlatformId = result.metadata.platform,
                        DataForDate = dateOfData,
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetricByOS.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;
                    campaignMetricExisting.OS = result.metadata.name;
                    campaignMetricExisting.Platform = result.metadata.platform;
                    campaignMetricExisting.ExchangeOSId = result.metadata.name;
                    campaignMetricExisting.ExchangePlatformId = result.metadata.platform;

                    _context.CampaignMetricByOS.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpsertCampaignMetricByBrowser(CampaignResponseObject campaignResponseObject, DateOnly dateOfData)
        {
            if (campaignResponseObject.results == null)
                return;
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetricByBrowsers
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == dateOfData
                        && x.Platform.ToUpper() == result.metadata.platform.ToUpper()
                        && x.Browser.ToUpper() == result.metadata.name.ToUpper()
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetricByBrowser
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        Browser = result.metadata.name,
                        Platform = result.metadata.platform,
                        ExchangeBrowserId = result.metadata.name,
                        ExchangePlatformId = result.metadata.platform,
                        DataForDate = dateOfData,
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetricByBrowsers.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;
                    campaignMetricExisting.Browser = result.metadata.name;
                    campaignMetricExisting.Platform = result.metadata.platform;
                    campaignMetricExisting.ExchangeBrowserId = result.metadata.name;
                    campaignMetricExisting.ExchangePlatformId = result.metadata.platform;

                    _context.CampaignMetricByBrowsers.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpsertCampaignMetricByRegions(CampaignResponseObject campaignResponseObject, DateOnly dateOfData)
        {
            if (campaignResponseObject.results == null)
                return;
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetricByRegions
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == dateOfData
                        && x.ExchangeRegionName.ToUpper() == result.metadata.name.ToUpper()
                        && x.ExchangeRegionCode.ToUpper() == result.metadata.code.ToUpper()
                        && x.ExchangeRegionId.ToUpper() == result.metadata.id.ToUpper()
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetricByRegion
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        Region = result.metadata.name,
                        RegionId = result.metadata.id,
                        RegionCode = result.metadata.code,
                        ExchangeRegionName = result.metadata.name,
                        ExchangeRegionCode = result.metadata.code,
                        ExchangeRegionId = result.metadata.id,
                        DataForDate = dateOfData,
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetricByRegions.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;
                    campaignMetricExisting.Region = result.metadata.name;
                    campaignMetricExisting.RegionId = result.metadata.id;
                    campaignMetricExisting.RegionCode = result.metadata.code;
                    campaignMetricExisting.ExchangeRegionName = result.metadata.name;
                    campaignMetricExisting.ExchangeRegionCode = result.metadata.code;
                    campaignMetricExisting.ExchangeRegionId = result.metadata.id;

                    _context.CampaignMetricByRegions.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpsertCampaignMetricByDomains(CampaignResponseObject campaignResponseObject, DateOnly dateOfData)
        {
            if (campaignResponseObject.results == null)
                return;
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetricByDomains
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == dateOfData
                        && x.ExchangeDomainName.ToUpper() == result.metadata.name.ToUpper()
                        && x.ExchangeDomainId.ToUpper() == result.metadata.id.ToUpper()
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetricByDomain
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        Domain = result.metadata.name,
                        ExchangeDomainName = result.metadata.name,
                        ExchangeDomainId = result.metadata.id,
                        DataForDate = dateOfData,
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetricByDomains.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;
                    campaignMetricExisting.Domain = result.metadata.name;
                    campaignMetricExisting.ExchangeDomainId = result.metadata.id;
                    campaignMetricExisting.ExchangeDomainName = result.metadata.name;

                    _context.CampaignMetricByDomains.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpsertCampaignMetricByAds(CampaignResponseObject campaignResponseObject, DateOnly dateOfData)
        {
            if (campaignResponseObject.results == null)
                return;
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                await InsertExchangeCreativeIfNotExists(result, campaignResponseObject.CampaignDetails);

                //check if record exists for camapignId, ExchangeId and DataForDate
                var campaignMetricExistingList = await _context.CampaignMetricByAds
                    .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                        && x.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                        && x.DataForDate == dateOfData
                        && x.ExchangeCreativeId.ToUpper() == result.metadata.id.ToUpper()
                        //&& x.ExchangeDomainId.ToUpper() == result.metadata.id.ToUpper()
                        && x.IsActive == true).ToListAsync();
                if (campaignMetricExistingList.Count == 0)
                {
                    var campaignMetric = new CampaignMetricByAd
                    {
                        ClientID = campaignResponseObject.CampaignDetails.ClientId,
                        CampaignId = campaignResponseObject.CampaignDetails.CampaignId,
                        ExchangeId = campaignResponseObject.CampaignDetails.ExchangeId,
                        ExchangeCreativeId = result.metadata.id,
                        DataForDate = dateOfData,
                        Impressions = result.metrics.impressions,
                        Clicks = result.metrics.clicks,
                        Ctr = result.metrics.ctr,
                        Spend = result.metrics.spend,
                        Ecpc = result.metrics.ecpc,
                        TotalConversions = result.metrics.totalConversions,
                        Conversions = result.metrics.conversions,
                        ViewConversions = result.metrics.viewConversions,
                        Cpa = result.metrics.cpa,
                        TotalCpa = result.metrics.totalCpa,
                        TotalSumValue = result.metrics.totalSumValue,
                        SumValue = result.metrics.sumValue,
                        ViewSumValue = result.metrics.viewSumValue,
                        TotalAverageValue = result.metrics.totalAverageValue,
                        AverageValue = result.metrics.averageValue,
                        ViewAverageValue = result.metrics.viewAverageValue,
                        Roas = result.metrics.roas,
                        TotalRoas = result.metrics.totalRoas,
                        IsActive = true,
                        CreatedBy = "Vikas",
                        CreatedById = 1,
                        CreatedOn = DateTime.Now

                    };
                    await _context.CampaignMetricByAds.AddAsync(campaignMetric);
                }
                else
                {
                    var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                    campaignMetricExisting.Impressions = result.metrics.impressions;
                    campaignMetricExisting.Clicks = result.metrics.clicks;
                    campaignMetricExisting.Ctr = result.metrics.ctr;
                    campaignMetricExisting.Spend = result.metrics.spend;
                    campaignMetricExisting.Ecpc = result.metrics.ecpc;
                    campaignMetricExisting.TotalConversions = result.metrics.totalConversions;
                    campaignMetricExisting.Conversions = result.metrics.conversions;
                    campaignMetricExisting.ViewConversions = result.metrics.viewConversions;
                    campaignMetricExisting.Cpa = result.metrics.cpa;
                    campaignMetricExisting.TotalCpa = result.metrics.totalCpa;
                    campaignMetricExisting.TotalSumValue = result.metrics.totalSumValue;
                    campaignMetricExisting.SumValue = result.metrics.sumValue;
                    campaignMetricExisting.ViewSumValue = result.metrics.viewSumValue;
                    campaignMetricExisting.TotalAverageValue = result.metrics.totalAverageValue;
                    campaignMetricExisting.AverageValue = result.metrics.averageValue;
                    campaignMetricExisting.ViewAverageValue = result.metrics.viewAverageValue;
                    campaignMetricExisting.Roas = result.metrics.roas;
                    campaignMetricExisting.TotalRoas = result.metrics.totalRoas;
                    campaignMetricExisting.IsActive = true;
                    campaignMetricExisting.LastModifiedBy = "Vikas";
                    campaignMetricExisting.LastModifiedById = 1;
                    campaignMetricExisting.LastModifiedOn = DateTime.Now;
                    campaignMetricExisting.ExchangeCreativeId = result.metadata.id;

                    _context.CampaignMetricByAds.Update(campaignMetricExisting);
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpdateConsolidatedCampaignData(CampaignResponseObject campaignResponseObject)
        {
            //get consolidated numbers from CampaignMetric table, for all dates, for a campaign & update Campaigns table metrics columns & also update ExchangeCampaign table metrics columns
            var campaignMetricExistingList = await _context.CampaignMetrics.AsNoTracking()
                .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId
                && x.IsActive == true).ToListAsync();
            var camp = await _context.Campaign
                .Where(x => x.Id == campaignResponseObject.CampaignDetails.CampaignId).FirstOrDefaultAsync();
            var xCamp = await _context.ExchangeCampaigns
                .Where(x => x.CampaignId == campaignResponseObject.CampaignDetails.CampaignId).ToListAsync();

            if (campaignMetricExistingList.Count > 0)
            {
                camp.TotalImpressions = campaignMetricExistingList.Sum(x => x.Impressions);
                camp.TotalClicks = campaignMetricExistingList.Sum(x => x.Clicks);
                camp.TotalSpend = campaignMetricExistingList.Sum(x => x.Spend);
                camp.TotalEcpc = campaignMetricExistingList.Average(x => x.Ecpc);
                camp.TotalCtr = campaignMetricExistingList.Average(x => x.Ctr);
                _context.Campaign.Update(camp);

                //foreach on ExchangeId in xCamp List
                foreach (var xCampItem in xCamp)
                {
                    var campaignMetricExistingListExchange = campaignMetricExistingList.Where(x => x.ExchangeId == xCampItem.ExchangeId).ToList();
                    if (campaignMetricExistingListExchange.Count > 0)
                    {
                        xCampItem.TotalImpressions = campaignMetricExistingListExchange.Where(y => y.ExchangeId == xCampItem.ExchangeId).Sum(x => x.Impressions);
                        xCampItem.TotalClicks = campaignMetricExistingListExchange.Where(y => y.ExchangeId == xCampItem.ExchangeId).Sum(x => x.Clicks);
                        xCampItem.TotalSpend = campaignMetricExistingListExchange.Where(y => y.ExchangeId == xCampItem.ExchangeId).Sum(x => x.Spend);
                        xCampItem.TotalCtr = campaignMetricExistingListExchange.Where(y => y.ExchangeId == xCampItem.ExchangeId).Average(x => x.Ctr);
                        xCampItem.TotalEcpc = campaignMetricExistingListExchange.Where(y => y.ExchangeId == xCampItem.ExchangeId).Average(x => x.Ecpc);
                        _context.ExchangeCampaigns.Update(xCampItem);
                    }
                }
                await _context.SaveChangesAsync();
            }
        }
        public async Task UpsertConsolidatedAdsData(CampaignResponseObject campaignResponseObject)
        {
            //Write code to insert records in CampaignMetric
            foreach (var result in campaignResponseObject.results)
            {
                var XchgCreativeRows = await _context.CampaignMetricByAds.AsNoTracking()
                    .Where(y => y.ExchangeId == campaignResponseObject.CampaignDetails.ExchangeId
                                && y.ExchangeCreativeId.ToUpper() == result.metadata.id.ToUpper())
                    .ToListAsync();
                var XchgCreative = await _context.ExchangeCreatives
                    .Where(y => y.ExchangeCreativeId.ToUpper() == result.metadata.id.ToUpper()).FirstOrDefaultAsync();
                if (XchgCreativeRows.Count > 0)
                {
                    XchgCreative.Impressions = XchgCreativeRows.Sum(x => x.Impressions);
                    XchgCreative.Clicks = XchgCreativeRows.Sum(x => x.Clicks);
                    XchgCreative.Spend = XchgCreativeRows.Sum(x => x.Spend);
                    XchgCreative.Ctr = XchgCreativeRows.Average(x => x.Ctr);
                    XchgCreative.Ecpc = XchgCreativeRows.Average(x => x.Ecpc);
                    XchgCreative.LastModifiedBy = "Vikas";
                    XchgCreative.LastModifiedById = 1;
                    XchgCreative.LastModifiedOn = DateTime.Now;
                    _context.ExchangeCreatives.Update(XchgCreative);
                }
            }
            await _context.SaveChangesAsync();
        }
        #endregion

        #region Upsert mgid data

        public async Task AddOrUpdateCampaignMetricByBrowser(CampaignMetricByBrowser metric)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetricByBrowsers
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.Browser.ToUpper() == metric.Browser.ToUpper()
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetricByBrowsers.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetricByBrowsers.Update(campaignMetricExisting);
            }
            await _context.SaveChangesAsync();
        }

        public async Task AddOrUpdateCampaignMetricByCountry(CampaignMetricByCountry metric)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetricByCountries
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.Country.ToUpper() == metric.Country.ToUpper()
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetricByCountries.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetricByCountries.Update(campaignMetricExisting);
            }
            await _context.SaveChangesAsync();
        }

        public async Task AddOrUpdateCampaignMetricByDomain(CampaignMetricByDomain metric)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetricByDomains
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.Domain.ToUpper() == metric.Domain.ToUpper()
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetricByDomains.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetricByDomains.Update(campaignMetricExisting);
            }
            await _context.SaveChangesAsync();

        }

        public async Task AddOrUpdateCampaignMetricByOs(CampaignMetricByOS metric)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetricByOS
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.OS.ToUpper() == metric.OS.ToUpper()
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetricByOS.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetricByOS.Update(campaignMetricExisting);
            }
            await _context.SaveChangesAsync();
        }

        public async Task AddOrUpdateCampaignMetricByRegion(CampaignMetricByRegion metric)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetricByRegions
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.Region.ToUpper() == metric.Region.ToUpper()
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetricByRegions.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.ExchangeRegionName = metric.ExchangeRegionName;
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetricByRegions.Update(campaignMetricExisting);
            }
            await _context.SaveChangesAsync();

        }

        public async Task AddOrUpdateCampaignMetricByAds(CampaignMetricByAd metric, string CreativeTitle)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetricByAds
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.ExchangeCreativeId.ToUpper() == metric.ExchangeCreativeId.ToUpper()
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetricByAds.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetricByAds.Update(campaignMetricExisting);
            }

            //Also check if record exist in ExchangeCreatives
            var exchangeCreative = await _context.ExchangeCreatives.AsNoTracking()
                .Where(x => x.ExchangeCreativeId.ToUpper() == metric.ExchangeCreativeId.ToUpper() && x.CampaignId == metric.CampaignId).FirstOrDefaultAsync();
            if (exchangeCreative == null)
            {
                exchangeCreative = new ExchangeCreative
                {
                    CampaignId = metric.CampaignId,
                    ExchangeCreativeId = metric.ExchangeCreativeId,
                    ExchangeId = metric.ExchangeId,
                    ContentTitle = CreativeTitle,
                    CreativeFormat = "",
                    ImageURL = "",
                    IsActive = true,
                    CreatedBy = "Vikas",
                    CreatedById = 1,
                    CreatedOn = DateTime.Now
                };
                await _context.ExchangeCreatives.AddAsync(exchangeCreative);
            }

            await _context.SaveChangesAsync();

        }

        public async Task AddOrUpdateCampaignMetricByDate(CampaignMetric metric)
        {
            //check if record exists for camapignId, ExchangeId and DataForDate
            var campaignMetricExistingList = await _context.CampaignMetrics
                .Where(x => x.CampaignId == metric.CampaignId
                    && x.ExchangeId == metric.ExchangeId
                    && x.DataForDate == metric.DataForDate
                    && x.IsActive == true).ToListAsync();
            if (campaignMetricExistingList.Count == 0)
            {
                await _context.CampaignMetrics.AddAsync(metric);
            }
            else
            {
                var campaignMetricExisting = campaignMetricExistingList.FirstOrDefault();
                campaignMetricExisting.Impressions = metric.Impressions;
                campaignMetricExisting.Clicks = metric.Clicks;
                campaignMetricExisting.Ctr = metric.Ctr;
                campaignMetricExisting.Spend = metric.Spend;
                campaignMetricExisting.Ecpc = metric.Ecpc;
                campaignMetricExisting.TotalConversions = metric.TotalConversions;
                campaignMetricExisting.Conversions = metric.Conversions;
                campaignMetricExisting.ViewConversions = metric.ViewConversions;
                campaignMetricExisting.Cpa = metric.Cpa;
                campaignMetricExisting.TotalCpa = metric.TotalCpa;
                campaignMetricExisting.TotalSumValue = metric.TotalSumValue;
                campaignMetricExisting.SumValue = metric.SumValue;
                campaignMetricExisting.ViewSumValue = metric.ViewSumValue;
                campaignMetricExisting.TotalAverageValue = metric.TotalAverageValue;
                campaignMetricExisting.AverageValue = metric.AverageValue;
                campaignMetricExisting.ViewAverageValue = metric.ViewAverageValue;
                campaignMetricExisting.Roas = metric.Roas;
                campaignMetricExisting.TotalRoas = metric.TotalRoas;
                campaignMetricExisting.IsActive = true;
                campaignMetricExisting.LastModifiedBy = "Vikas";
                campaignMetricExisting.LastModifiedById = 1;
                campaignMetricExisting.LastModifiedOn = DateTime.Now;

                _context.CampaignMetrics.Update(campaignMetricExisting);
            }
            await _context.SaveChangesAsync();

        }
        #endregion

        public async Task<IList<Campaign>> FetchCampaignMetricsByDates(DateOnly fromDate, DateOnly toDate, List<int> campaignIdsList, List<int> clientIdsList)
        {
            var campaignQuery = _context.Campaign
                .Where(y => y.IsActive == true);

            if (campaignIdsList[0] > 0)
            {
                campaignQuery = campaignQuery.Where(x => campaignIdsList.Contains(x.Id));
            }
            if (clientIdsList.Count > 0 && clientIdsList[0] > 0)
            {
                campaignQuery = campaignQuery.Where(x => clientIdsList.Contains(x.CustomerId));
            }

            try
            {
                var campaigns = await campaignQuery.Include(x => x.CampaignMetrics.Where(x => x.DataForDate >= fromDate && x.DataForDate <= toDate && x.IsActive == true))
                    .AsNoTracking()
                    .ToListAsync();
                return campaigns;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }

            return null;

        }
        public async Task<CampaignResultDTO> GetCampaignResultAsync(CampaignResultRequest request)
        {

            if (request.From.IsNullOrEmpty())
                request.From = DateOnly.FromDateTime((DateTime.Now.AddDays(-14))).ToString("yyyy-MM-dd");
            if (request.To.IsNullOrEmpty())
                request.To = DateOnly.FromDateTime((DateTime.Now)).ToString("yyyy-MM-dd");
            DateOnly fromDate = DateOnly.FromDateTime(DateTime.Parse(request.From));
            DateOnly toDate = DateOnly.FromDateTime(DateTime.Parse(request.To));

            Campaign campaign = await FetchCampaignMetricsForFilter(request, fromDate, toDate);
            if (campaign == null)
            {
                return null;
            }

            var summary = FetchMetricsSummary(campaign, request);
            CampaignResultDTO dto = new CampaignResultDTO();
            dto.CampaignResultRequest = request;
            dto.PerformanceSummary = summary;
            dto.Campaign = campaign;
            dto.ChartingData = fetchChartingData(dto);
            dto.Summary = fetchSummaryData(dto);

            return dto;
        }
        public async Task<List<MGIDDomainMapping>> GetMGIDDomainMappings()
        {
            return await _context.MgidDomainMappings.AsNoTracking().ToListAsync();
        }
        public async Task<List<CampaignMetric>> GetMGIDCampaignMetric(DateOnly StartDate, DateOnly EndDate
            , int MGIDExchangeId, int CampaignId)
        {
            return await _context.CampaignMetrics.AsNoTracking()
                .Where(y => y.ExchangeId == MGIDExchangeId && y.CampaignId == CampaignId
                            && y.DataForDate >= StartDate && y.DataForDate <= EndDate).ToListAsync();
        }

        public async Task<bool> UpdateImpressionInDomainsDataForExchange(DateOnly Fordate, int ExchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            => await UpdateMetricsAsync<CampaignMetricByDomain>(Fordate, ExchangeId, countOfImpressions, countOfClicks, daysCTR);

        public async Task<bool> UpdateImpressionInRegionsDataForExchange(DateOnly Fordate, int ExchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            => await UpdateMetricsAsync<CampaignMetricByRegion>(Fordate, ExchangeId, countOfImpressions, countOfClicks, daysCTR);

        public async Task<bool> UpdateImpressionInCountriesDataForExchange(DateOnly Fordate, int ExchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            => await UpdateMetricsAsync<CampaignMetricByCountry>(Fordate, ExchangeId, countOfImpressions, countOfClicks, daysCTR);

        public async Task<bool> UpdateImpressionInOSDataForExchange(DateOnly Fordate, int ExchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            => await UpdateMetricsAsync<CampaignMetricByOS>(Fordate, ExchangeId, countOfImpressions, countOfClicks, daysCTR);

        public async Task<bool> UpdateImpressionInBrowersDataForExchange(DateOnly Fordate, int ExchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            => await UpdateMetricsAsync<CampaignMetricByBrowser>(Fordate, ExchangeId, countOfImpressions, countOfClicks, daysCTR);

        public async Task<bool> UpdateImpressionInAdsDataForExchange(DateOnly Fordate, int ExchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            => await UpdateMetricsAsync<CampaignMetricByAd>(Fordate, ExchangeId, countOfImpressions, countOfClicks, daysCTR);
        public async Task<bool> UpdateMGIDDomainMapping(MGIDDomainMapDTO mapping)
        {
            var m = await _context.MgidDomainMappings.Where(y => y.DomainCode.Trim() == mapping.DomainCode)
                .FirstOrDefaultAsync();
            if (m == null)
            {
                m = new MGIDDomainMapping();
                m.DomainCode = mapping.DomainCode;
                m.DomainName = mapping.DomainName.Trim();
                _context.MgidDomainMappings.Add(m);
                await _context.SaveChangesAsync();
                return true;
            }
            else
            {
                m.DomainName = mapping.DomainName.Trim();
                await _context.SaveChangesAsync();
            }
            return false;
        }

        #region private methods
        private async Task<bool> UpdateMetricsAsync<T>(DateOnly forDate, int exchangeId, double countOfImpressions, double countOfClicks, double daysCTR)
            where T : BaseMetric
        {
            var metricsData = await _context.Set<T>()
                .Where(y => y.ExchangeId == exchangeId && y.DataForDate == forDate && y.IsActive == true)
                .ToListAsync();

            if (metricsData.Count() == 0) return true;

            foreach (var d in metricsData)
            {
                d.Impressions = Math.Round((d.Clicks / countOfClicks) * countOfImpressions, 0);
                d.Ctr = daysCTR;
            }

            await _context.SaveChangesAsync();
            return true;
        }
        //Create private function to insert record in ExchangeCreative if no row exists for ExangeCreativeId, parameter passed is the result JSON having id parameter with value of ExchagneCreativeID
        private async Task InsertExchangeCreativeIfNotExists(Result result, Models.CampaignsToSync campaignDetails)
        {
            //check if creative exists in ExchangeCreative table
            var exchangeCreativeId = result.metadata.id;
            var exchangeCreative = await _context.ExchangeCreatives.AsNoTracking()
                .Where(x => x.ExchangeCreativeId.ToUpper() == exchangeCreativeId.ToUpper() && x.CampaignId == campaignDetails.CampaignId).FirstOrDefaultAsync();
            if (exchangeCreative == null)
            {
                exchangeCreative = new ExchangeCreative
                {
                    CampaignId = campaignDetails.CampaignId,
                    ExchangeCreativeId = exchangeCreativeId,
                    ExchangeId = campaignDetails.ExchangeId,
                    ContentTitle = result.metadata.title,
                    CreativeFormat = result.metadata.creativeFormat,
                    ImageURL = result.metadata.url,
                    IsActive = true,
                    CreatedBy = "Vikas",
                    CreatedById = 1,
                    CreatedOn = DateTime.Now
                };
                await _context.ExchangeCreatives.AddAsync(exchangeCreative);
                await _context.SaveChangesAsync();
            }
        }
        private IList<CampaignPerformanceSummary> FetchMetricsSummary(Campaign campaign, CampaignResultRequest request)
        {
            IList<CampaignPerformanceSummary> summary = new List<CampaignPerformanceSummary>();
            switch (request.ResultFilter)
            {
                default:
                case ("date"):
                    if (campaign.CampaignMetrics != null)
                    {
                        foreach (var m in campaign.CampaignMetrics.OrderBy(y => y.DataForDate))
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = "",
                                DataForDate = m.DataForDate,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;
                case ("browser"):
                    if (campaign.CampaignMetricsByBrowsers != null)
                    {
                        var grouped = campaign.CampaignMetricsByBrowsers.GroupBy(y => y.Browser)
                            .Select(g => new
                            {
                                Browser = g.Key,
                                Impressions = g.Sum(x => x.Impressions),
                                Clicks = g.Sum(x => x.Clicks),
                                Ctr = g.Average(x => x.Ctr),
                                Spend = g.Sum(x => x.Spend),
                                Ecpc = g.Average(x => x.Ecpc)
                            });
                        foreach (var m in grouped)
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = m.Browser,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;

                case ("os"):
                    if (campaign.CampaignMetricByOS != null)
                    {
                        var grouped = campaign.CampaignMetricByOS.GroupBy(y => y.OS)
                            .Select(g => new
                            {
                                OS = g.Key,
                                Impressions = g.Sum(x => x.Impressions),
                                Clicks = g.Sum(x => x.Clicks),
                                Ctr = g.Average(x => x.Ctr),
                                Spend = g.Sum(x => x.Spend),
                                Ecpc = g.Average(x => x.Ecpc)
                            });

                        foreach (var m in grouped)
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = m.OS,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;
                case ("region"):
                    if (campaign.CampaignMetricByRegions != null)
                    {
                        var grouped = campaign.CampaignMetricByRegions.GroupBy(y => y.Region)
                            .Select(g => new
                            {
                                FilterByValue = g.Key,
                                Impressions = g.Sum(x => x.Impressions),
                                Clicks = g.Sum(x => x.Clicks),
                                Ctr = g.Average(x => x.Ctr),
                                Spend = g.Sum(x => x.Spend),
                                Ecpc = g.Average(x => x.Ecpc)
                            });

                        foreach (var m in grouped)
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = m.FilterByValue,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;
                case ("country"):
                    if (campaign.CampaignMetricByCountries != null)
                    {
                        var grouped = campaign.CampaignMetricByCountries.GroupBy(y => y.Country)
                            .Select(g => new
                            {
                                Country = g.Key,
                                Impressions = g.Sum(x => x.Impressions),
                                Clicks = g.Sum(x => x.Clicks),
                                Ctr = g.Average(x => x.Ctr),
                                Spend = g.Sum(x => x.Spend),
                                Ecpc = g.Average(x => x.Ecpc)
                            });

                        foreach (var m in grouped)
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = m.Country,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;
                case ("domain"):
                    if (campaign.CampaignMetricByDomains != null)
                    {
                        var grouped = campaign.CampaignMetricByDomains.GroupBy(y => y.Domain)
                            .Select(g => new
                            {
                                Domain = g.Key,
                                Impressions = g.Sum(x => x.Impressions),
                                Clicks = g.Sum(x => x.Clicks),
                                Ctr = g.Average(x => x.Ctr),
                                Spend = g.Sum(x => x.Spend),
                                Ecpc = g.Average(x => x.Ecpc)
                            });

                        foreach (var m in grouped)
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = m.Domain,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;
                case ("ads"):
                    if (campaign.CampaignMetricByAds != null)
                    {
                        var grouped = campaign.CampaignMetricByAds.Where(y => y.CreativeId != null).GroupBy(y => y.Creative.Name)
                            .Select(g => new
                            {
                                Creative = g.Key,
                                Impressions = g.Sum(x => x.Impressions),
                                Clicks = g.Sum(x => x.Clicks),
                                Ctr = g.Average(x => x.Ctr),
                                Spend = g.Sum(x => x.Spend),
                                Ecpc = g.Average(x => x.Ecpc)
                            });

                        foreach (var m in grouped)
                        {
                            summary.Add(new CampaignPerformanceSummary
                            {
                                FilterByValue = m.Creative,
                                Clicks = m.Clicks,
                                Impressions = m.Impressions,
                                CTR = m.Ctr,
                                AmountSpent = !request.LimitedAccessUser ? m.Spend : (campaign.Sale.CPC * m.Clicks),
                                AvgCPC = !request.LimitedAccessUser ? m.Ecpc : campaign.Sale.CPC
                            });
                        }
                    }
                    break;
            }

            return summary;
        }
        private async Task<Campaign> FetchCampaignMetricsForFilter(CampaignResultRequest request, DateOnly fromDate, DateOnly toDate)
        {
            var query = _context.Campaign.AsNoTracking()
            .Where(y => y.CampaignGuid == request.CampaignGuid)
            .Include(y => y.Sale)
            .Include(y => y.Customer);
            Campaign result;
            switch (request.ResultFilter)
            {
                case ("date"):
                default:
                    result = await query.Include(y => y.CampaignMetrics.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                                    .FirstOrDefaultAsync();
                    break;
                case ("country"):
                    result = await query.Include(y => y.CampaignMetricByCountries.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                                    .FirstOrDefaultAsync();
                    if (result != null)
                    {
                        // Remove rows with 0 Impressions and 0 Clicks
                        result.CampaignMetricByCountries = result.CampaignMetricByCountries
                            .Where(x => x.Impressions >= 1000 || x.Clicks != 0)
                            .ToList();
                    }
                    break;
                case ("os"):
                    result = await query.Include(y => y.CampaignMetricByOS.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                        .FirstOrDefaultAsync();
                    break;
                case ("browser"):
                    result = await query.Include(y => y.CampaignMetricsByBrowsers.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                                    .FirstOrDefaultAsync();
                    break;
                case ("region"):
                    result = await query.Include(y => y.CampaignMetricByRegions.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                                    .FirstOrDefaultAsync();
                    if (result != null)
                    {
                        // Remove rows with 0 Impressions and 0 Clicks
                        result.CampaignMetricByRegions = result.CampaignMetricByRegions
                            .Where(x => x.Impressions >= 1000 || x.Clicks != 0)
                            .ToList();
                    }
                    break;
                case ("domain"):
                    result = await query.Include(y => y.CampaignMetricByDomains.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                                    .FirstOrDefaultAsync();
                    if (result != null)
                    {
                        // Remove rows with 0 Impressions and 0 Clicks
                        result.CampaignMetricByDomains = result.CampaignMetricByDomains
                            .Where(x => x.Impressions >= 1000 || x.Clicks != 0)
                            .ToList();
                    }
                    break;
                case ("ads"):
                    result = await query.Include(y => y.CampaignMetricByAds.Where(x => x.DataForDate >= fromDate
                            && x.DataForDate <= toDate && (request.ExchangeId == 0 || x.ExchangeId == request.ExchangeId)))
                        .ThenInclude(t => t.Creative)
                        .FirstOrDefaultAsync();
                    break;
            }
            if (result != null)
            {
                switch (request.ResultFilter)
                {
                    case ("date"):
                    default:
                        var grouped = new List<CampaignMetric>();
                        foreach (var m in result.CampaignMetrics)
                        {
                            var r = grouped.Where(y => y.DataForDate == m.DataForDate).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.ExchangeId = 0;
                                grouped.Add(r);
                            }
                        }
                        result.CampaignMetrics = grouped;
                        break;
                    case ("country"):
                        var groupedCountry = new List<CampaignMetricByCountry>();
                        string countryName = "India";
                        foreach (var m in result.CampaignMetricByCountries)
                        {
                            if (m.Country.Trim().ToUpper() == "INDIA")
                                countryName = "India";
                            else
                                countryName = "Outside India";

                            var r = groupedCountry.Where(y => y.Country == m.Country).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.Country = countryName;
                                r.ExchangeId = 0;
                                groupedCountry.Add(r);
                            }
                        }
                        result.CampaignMetricByCountries = groupedCountry.OrderByDescending(x => x.Clicks).ToList();
                        break;
                    case ("os"):
                        var groupedOS = new List<CampaignMetricByOS>();
                        var osName = "IOS";
                        foreach (var m in result.CampaignMetricByOS)
                        {
                            var toUpperOS = m.OS.Trim().ToUpper();
                            if (toUpperOS == "IOS" || toUpperOS.Contains(" IOS"))
                                osName = "IOS";
                            else if (toUpperOS.Contains(" ANDROID 6.") || toUpperOS.Contains(" ANDROID 7.") ||
                                     toUpperOS.Contains(" ANDROID 8.")
                                     || toUpperOS.Contains(" ANDROID 9.") || toUpperOS.Contains(" ANDROID 5."))
                                osName = "Andriod Under 10.x ";
                            else if (toUpperOS.Contains("ANDROID"))
                                osName = "Android 10+";
                            else if (toUpperOS.Contains("DESKTOP"))
                                osName = "Desktop";
                            var r = groupedOS.Where(y => y.OS == m.OS).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.OS = osName;
                                r.ExchangeId = 0;
                                groupedOS.Add(r);
                            }
                        }
                        result.CampaignMetricByOS = groupedOS.OrderByDescending(x => x.Clicks).ToList();
                        break;
                    case ("browser"):
                        var groupedBrowser = new List<CampaignMetricByBrowser>();
                        foreach (var m in result.CampaignMetricsByBrowsers)
                        {
                            string browesrName = "InApp";
                            if (m.Browser.Trim().ToUpper() == "INAPP" || m.Browser.Trim().ToUpper() == "WEBVIEW")
                                browesrName = "InApp";
                            else
                                browesrName = "InWeb";

                            var r = groupedBrowser.Where(y => y.Browser == browesrName).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.Browser = browesrName;
                                r.ExchangeId = 0;
                                groupedBrowser.Add(r);
                            }
                        }
                        result.CampaignMetricsByBrowsers = groupedBrowser.OrderByDescending(x => x.Clicks).ToList();
                        break;
                    case ("region"):
                        var groupedRegion = new List<CampaignMetricByRegion>();
                        foreach (var m in result.CampaignMetricByRegions)
                        {
                            var r = groupedRegion.Where(y => y.Region == m.Region).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.ExchangeId = 0;
                                groupedRegion.Add(r);
                            }
                        }
                        result.CampaignMetricByRegions = groupedRegion.OrderByDescending(x => x.Clicks).ToList();
                        break;
                    case ("domain"):
                        var groupedDomain = new List<CampaignMetricByDomain>();
                        foreach (var m in result.CampaignMetricByDomains)
                        {
                            var r = groupedDomain.Where(y => y.Domain == m.Domain).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.ExchangeId = 0;
                                groupedDomain.Add(r);
                            }
                        }
                        result.CampaignMetricByDomains = groupedDomain.OrderByDescending(x => x.Clicks).ToList();
                        break;
                    case ("ads"):
                        var groupedAds = new List<CampaignMetricByAd>();
                        foreach (var m in result.CampaignMetricByAds)
                        {
                            var r = groupedAds.Where(y => y.CreativeId == m.CreativeId).FirstOrDefault();
                            if (r != null)
                            {
                                r.Impressions += m.Impressions;
                                r.Clicks += m.Clicks;
                                r.Spend += m.Spend;
                                r.Ecpc = (r.Ecpc + m.Ecpc) / 2;
                                r.Ctr = (r.Ctr + m.Ctr) / 2;
                            }
                            else
                            {
                                r = m;
                                r.ExchangeId = 0;
                                groupedAds.Add(r);
                            }
                        }
                        result.CampaignMetricByAds = groupedAds.OrderByDescending(x => x.Clicks).ToList(); ;
                        break;
                }
            }
            return result;
        }
        private ChartingDataResponseDTO fetchChartingData(CampaignResultDTO dto)
        {
            ChartingDataResponseDTO responseDTO = new ChartingDataResponseDTO();
            ChartingDataForSingleCampaignDTO c = new ChartingDataForSingleCampaignDTO();

            switch (dto.CampaignResultRequest.ResultFilter)
            {
                default:
                case ("date"):
                    responseDTO.Labels = dto.PerformanceSummary.OrderBy(y => y.DataForDate).Select(x => x.DataForDate.ToString("dd-MM-yyyy")).ToList();

                    c.CampaignId = dto.Campaign.Id;
                    c.CampaignGuid = dto.Campaign.CampaignGuid.Value;
                    c.CampaignName = dto.Campaign.Name;
                    c.Impressions = dto.PerformanceSummary.OrderBy(y => y.DataForDate).Select(x => x.Impressions).ToList();
                    c.Clicks = dto.PerformanceSummary.OrderBy(y => y.DataForDate).Select(x => x.Clicks).ToList();
                    c.Spend = dto.PerformanceSummary.OrderBy(y => y.DataForDate).Select(x => x.AmountSpent).ToList();
                    c.CTR = dto.PerformanceSummary.OrderBy(y => y.DataForDate).Select(x => x.CTR).ToList();
                    c.AvgCPC = dto.PerformanceSummary.OrderBy(y => y.DataForDate).Select(x => x.AvgCPC).ToList();
                    responseDTO.CampaignResponse.Add(c);
                    break;
                case ("browser"):
                case ("os"):
                case ("country"):
                case ("ads"):


                    responseDTO.Labels = dto.PerformanceSummary.OrderBy(y => y.FilterByValue).Select(x => x.FilterByValue).ToList();

                    c.CampaignId = dto.Campaign.Id;
                    c.CampaignName = dto.Campaign.Name;
                    c.Impressions = dto.PerformanceSummary.OrderBy(y => y.FilterByValue).Select(x => x.Impressions).ToList();
                    c.Clicks = dto.PerformanceSummary.OrderBy(y => y.FilterByValue).Select(x => x.Clicks).ToList();
                    c.Spend = dto.PerformanceSummary.OrderBy(y => y.FilterByValue).Select(x => x.AmountSpent).ToList();
                    c.CTR = dto.PerformanceSummary.OrderBy(y => y.FilterByValue).Select(x => x.CTR).ToList();
                    c.AvgCPC = dto.PerformanceSummary.OrderBy(y => y.FilterByValue).Select(x => x.AvgCPC).ToList();
                    responseDTO.CampaignResponse.Add(c);
                    break;
                case ("domain"):

                    responseDTO.Labels = dto.PerformanceSummary.OrderByDescending(y => y.Clicks)
                        .Take(25)
                        .Select(x => x.FilterByValue.Length > 21 ? x.FilterByValue.Substring(0, 20) : x.FilterByValue)
                        .ToList();

                    c.CampaignId = dto.Campaign.Id;
                    c.CampaignName = dto.Campaign.Name;
                    c.Impressions = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.Impressions).ToList();
                    c.Clicks = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.Clicks).ToList();
                    c.Spend = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.AmountSpent).ToList();
                    c.CTR = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.CTR).ToList();
                    c.AvgCPC = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.AvgCPC).ToList();
                    responseDTO.CampaignResponse.Add(c);
                    break;
                case ("region"):
                    responseDTO.Labels = dto.PerformanceSummary.OrderByDescending(y => y.Clicks)
                       .Take(25)
                       .Select(x => x.FilterByValue.Length > 21 ? x.FilterByValue.Substring(0, 20) : x.FilterByValue)
                       .ToList();


                    c.CampaignId = dto.Campaign.Id;
                    c.CampaignName = dto.Campaign.Name;
                    c.Impressions = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.Impressions).ToList();
                    c.Clicks = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.Clicks).ToList();
                    c.Spend = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.AmountSpent).ToList();
                    c.CTR = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.CTR).ToList();
                    c.AvgCPC = dto.PerformanceSummary.OrderByDescending(y => y.Clicks).Take(25).Select(x => x.AvgCPC).ToList();
                    responseDTO.CampaignResponse.Add(c);
                    break;
            }



            return responseDTO;
        }
        private SummaryDTO fetchSummaryData(CampaignResultDTO dto)
        {
            //return SummaryDTO with summary from PerformanceSummary object
            SummaryDTO summary = new SummaryDTO();
            summary.Clicks = dto.PerformanceSummary.Count == 0 ? 0 : dto.PerformanceSummary.Sum(x => x.Clicks);
            summary.Impressions = dto.PerformanceSummary.Count == 0 ? 0 : dto.PerformanceSummary.Sum(x => x.Impressions);
            summary.AmountSpent = dto.PerformanceSummary.Count == 0 ? 0 : Math.Round(dto.PerformanceSummary.Sum(x => x.AmountSpent), 2);
            summary.CTR = dto.PerformanceSummary.Count == 0 ? 0 : Math.Round(dto.PerformanceSummary.Average(x => x.CTR), 2);
            summary.AverageCPC = dto.PerformanceSummary.Count == 0 ? 0 : Math.Round(dto.PerformanceSummary.Average(x => x.AvgCPC), 2);
            return summary;
        }
        #endregion
    }
}
