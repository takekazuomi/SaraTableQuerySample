using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;
using System.Data.Services.Client;
using System.Linq.Expressions;
using Common.Logging;
using System.Collections;


namespace TableQuery
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.Naming",
        "CA1710:IdentifiersShouldHaveCorrectSuffix",
        Justification = "The intent is to mirror the name of the DataServiceQuery type.")]
    public class SaraTableQuery<T> : IQueryable<T>
    {
        private CloudTableQuery<T> query;


       public SaraTableQuery(DataServiceQuery<T> query, RetryPolicy policy)
        {
            this.query = new CloudTableQuery<T>(query, policy);
        }

        public IQueryProvider Provider
        {
            get
            {
                return new SaraQueryProvider(query.Provider);
            }
        }

        public IEnumerator GetEnumerator()
        {
            return query.GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return query.GetEnumerator();
        }

        public Type ElementType
        {
            get {
                return query.ElementType;
            }
        }

        public Expression Expression
        {
            get {
                return query.Expression;
            }
        }

        public IEnumerable<T> Execute()
        {
            return query.Execute();
        }


        public IEnumerable<T> Execute(ResultContinuation continuationToken)
        {
            return query.Execute(continuationToken);
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
            return this.provider.CreateQuery<T>(expression).AsSaraTableQuery<T>(SaraRetryPolicies.DefaultRetry);
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
            var query1 = query as SaraTableQuery<TElement>;
            if (query1 != null)
            {
                return query1;
            }

            var query2 = query as DataServiceQuery<TElement>;
            if (query2 != null)
            {
                return new SaraTableQuery<TElement>(query2, retry);
            }

            throw new ArgumentException("only SaraTableQuery and DataServiceQuery acceptable","query");
        }

    }

    /*
    RetryPolicies            this.RetryPolicy = RetryPolicies.RetryExponential(RetryPolicies.DefaultClientRetryCount, RetryPolicies.DefaultClientBackoff);
                this.Timeout = TimeSpan.FromSeconds(90);

     */

    public static class SaraRetryPolicies
    {
        static readonly ILog logger = LogManager.GetCurrentClassLogger();

        public static readonly RetryPolicy DefaultRetry = SaraRetryPolicies.RetryExponential(RetryPolicies.DefaultClientRetryCount,
                    RetryPolicies.DefaultMinBackoff,
                    RetryPolicies.DefaultMaxBackoff,
                    RetryPolicies.DefaultClientBackoff
                    );


        public static RetryPolicy RetryExponential(int retryCount, TimeSpan minBackoff, TimeSpan maxBackoff, TimeSpan deltaBackoff)
        {

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
