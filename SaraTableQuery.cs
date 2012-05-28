using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;
using System.Data.Services.Client;
using System.Linq.Expressions;
using Common.Logging;


namespace TableQuery
{
    public class SaraTableQuery<T> : CloudTableQuery<T>
    {
        /*
         * public SaraTableQuery(DataServiceQuery<T> query)
                    : base(query, RetryPolicies.RetryExponential(3, TimeSpan.FromSeconds(2)))
                {
                }
        */

        public SaraTableQuery(DataServiceQuery<T> query, RetryPolicy policy)
            : base(query, policy)
        {
        }

        public new IQueryProvider Provider
        {
            get
            {
                return new SaraQueryProvider(base.Provider);
            }
        }
    }


    public class SaraQueryProvider : IQueryProvider
    {
        IQueryProvider provider;

        public SaraQueryProvider(IQueryProvider provider)
        {

            this.provider = provider;
        }

        public IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return this.provider.CreateQuery<T>(expression).AsTableServiceQuery<T>();
        }

        public IQueryable CreateQuery(Expression expression)
        {
            throw new NotImplementedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            return this.provider.Execute<TResult>(expression);
        }

        public object Execute(Expression expression)
        {
            return this.provider.Execute(expression);
        }
    }

    public static class SaraExtensionMethods
    {
        public static SaraTableQuery<TElement> AsSaraTableQuery<TElement>(this IQueryable<TElement> query, RetryPolicy retry)
        {
            return new SaraTableQuery<TElement>(query as DataServiceQuery<TElement>, retry);
        }
    }

    /*
    RetryPolicies            this.RetryPolicy = RetryPolicies.RetryExponential(RetryPolicies.DefaultClientRetryCount, RetryPolicies.DefaultClientBackoff);
                this.Timeout = TimeSpan.FromSeconds(90);

     */

    public static class SaraRetryPolicies
    {
        static readonly ILog logger = LogManager.GetCurrentClassLogger();

        public static RetryPolicy RetryExponential(int retryCount, TimeSpan minBackoff, TimeSpan maxBackoff, TimeSpan deltaBackoff)
        {
/*
 * CommonUtils.AssertInBounds("currentRetryCount", retryCount, 0, int.MaxValue);
 * CommonUtils.AssertInBounds("minBackoff", minBackoff, TimeSpan.Zero, TimeSpan.MaxValue);
            CommonUtils.AssertInBounds("maxBackoff", maxBackoff, TimeSpan.Zero, TimeSpan.MaxValue);
            CommonUtils.AssertInBounds("deltaBackoff", deltaBackoff, TimeSpan.Zero, TimeSpan.MaxValue);
*/ 

            return () =>
            {
                return (int currentRetryCount, Exception lastException, out TimeSpan retryInterval) =>
                {
                    var ret = true;

                    if (currentRetryCount < retryCount)
                    {
                        Random r = new Random();
                        int increment = (int)((Math.Pow(2, currentRetryCount) - 1) * r.Next((int)(deltaBackoff.TotalMilliseconds * 0.8), (int)(deltaBackoff.TotalMilliseconds * 1.2)));
                        int timeToSleepMsec = (int)Math.Min(minBackoff.TotalMilliseconds + increment, maxBackoff.TotalMilliseconds);

                        retryInterval = TimeSpan.FromMilliseconds(timeToSleepMsec);
                        ret = true;
                    }
                    else {
                        retryInterval = TimeSpan.Zero;
                        ret = false;
                    }
                    logger.Info(m => m("{0} {1} {2}", ret, currentRetryCount, lastException != null ? lastException.ToString() : "-"));
                    return ret;
                };
            };
        }
    }
}
