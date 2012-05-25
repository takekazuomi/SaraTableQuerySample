using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;
using System.Data.Services.Client;
using System.Linq.Expressions;


namespace TableQuery
{
    public class SaraTableQuery<T> : CloudTableQuery<T>
    {
        public SaraTableQuery(DataServiceQuery<T> query)
            : base(query, RetryPolicies.RetryExponential(3, TimeSpan.FromSeconds(2)))
        {
        }

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
        public static SaraTableQuery<TElement> AsSaraTableQuery<TElement>(this IQueryable<TElement> query)
        {
            return new SaraTableQuery<TElement>(query as DataServiceQuery<TElement>);
        }
    }
}
