using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;


/*

 * Partition key	Row key	Timestamp	TotalRequests	TotalBillableRequests	TotalIngress	TotalEgress	Availability	AverageE2ELatency	AverageServerLatency	PercentSuccess	PercentThrottlingError	PercentTimeoutError	PercentServerOtherError	PercentClientOtherError	PercentAuthorizationError	PercentNetworkError	Success	AnonymousSuccess	SASSuccess	ThrottlingError	AnonymousThrottlingError	SASThrottlingError	ClientTimeoutError	AnonymousClientTimeoutError	SASClientTimeoutError	ServerTimeoutError	AnonymousServerTimeoutError	SASServerTimeoutError	ClientOtherError	AnonymousClientOtherError	SASClientOtherError	ServerOtherError	AnonymousServerOtherError	SASServerOtherError	AuthorizationError	AnonymousAuthorizationError	SASAuthorizationError	NetworkError	AnonymousNetworkError	SASNetworkError
 * 20111203T1400	system;All	2011/12/03 15:22:23	2	2	21516	27946	100	34.5	30.5	100	0	0	0	0	0	0	2	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0

Partition key	Row key	Timestamp	TotalRequests	TotalBillableRequests	TotalIngress	TotalEgress	Availability	AverageE2ELatency	AverageServerLatency	PercentSuccess	PercentThrottlingError	PercentTimeoutError	PercentServerOtherError	PercentClientOtherError	PercentAuthorizationError	PercentNetworkError	Success	AnonymousSuccess	SASSuccess	ThrottlingError	AnonymousThrottlingError	SASThrottlingError	ClientTimeoutError	AnonymousClientTimeoutError	SASClientTimeoutError	ServerTimeoutError	AnonymousServerTimeoutError	SASServerTimeoutError	ClientOtherError	AnonymousClientOtherError	SASClientOtherError	ServerOtherError	AnonymousServerOtherError	SASServerOtherError	AuthorizationError	AnonymousAuthorizationError	SASAuthorizationError	NetworkError	AnonymousNetworkError	SASNetworkError
20111203T1400	user;QueryEntities	2011/12/03 15:22:22	3368	3368	2312972	122116544	100	40.744062	12.412708	100	0	0	0	0	0	0	3368	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0

*/


namespace TableQuery
{
    /// <summary>
    /// user;QueryEntities
    /// </summary>
    public class MetricsTransactionTable : TableServiceEntity
    {
        Int64 TotalRequestsl;
        Int64 TotalBillableRequests;
        Int64 TotalIngress;
        Int64 TotalEgress;
        Double Availability;
        Double AverageE2ELatency;
        Double AverageServerLatency;
        Double PercentSuccess;
        Double PercentSuccessOutsideSLA;
        Double PercentThrottlingError;
        Double PercentTimeoutError;
        Double PercentServerOtherError;
        Double PercentClientOtherError;
        Double PercentAuthorizationError;
        Double PercentNetworkError;
        Int64 Success;
        Int64 AnonymousSuccess;
        Int64 SASSuccess;
        Int64 SuccessOutsideSLA;
        Int64 AnonymousSuccessOutsideSLA;
        Int64 SASSuccessOutsideSLA;
        Int64 ThrottlingError;
        Int64 AnonymousThrottlingError;
        Int64 SASThrottlingError;
        Int64 ClientTimeoutError;
        Int64 AnonymousClientTimeoutError;
        Int64 SASClientTimeoutError;
        Int64 ServerTimeoutError;
        Int64 AnonymousServerTimeoutError;
        Int64 SASServerTimeoutError;
        Int64 ClientOtherError;
        Int64 AnonymousClientOtherError;
        Int64 SASClientOtherError;
        Int64 ServerOtherError;
        Int64 AnonymousServerOtherError;
        Int64 SASServerOtherError;
        Int64 AuthorizationError;
        Int64 AnonymousAuthorizationError;
        Int64 SASAuthorizationError;
        Int64 NetworkError;
        Int64 AnonymousNetworkError;
        Int64 SASNetworkError;
    }
}
