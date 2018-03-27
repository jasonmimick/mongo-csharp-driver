/* Copyright 2017-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Core.Operations
{
    internal static class RetryableWriteOperationExecutor
    {
        // public static methods
        public static TResult Execute<TResult>(IRetryableWriteOperation<TResult> operation, IWriteBinding binding, bool retryRequested, CancellationToken cancellationToken)
        {
            using (var context = RetryableWriteContext.Create(binding, retryRequested, cancellationToken))
            {
                return Execute(operation, context, cancellationToken);
            }
        }

        public static TResult Execute<TResult>(IRetryableWriteOperation<TResult> operation, RetryableWriteContext context, CancellationToken cancellationToken)
        {
            if (!context.RetryRequested || !AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                return operation.ExecuteAttempt(context, 1, null, cancellationToken);
            }

            var transactionNumber = context.Binding.Session.AdvanceTransactionNumber();
            Exception originalException;
            try
            {
                System.Console.WriteLine(" ***** " + "Execute" +" ***** ");
                return operation.ExecuteAttempt(context, 1, transactionNumber, cancellationToken);
            }
            catch (Exception ex) when (IsRetryableException(ex))
            {
                originalException = ex;
            }

            try
            {
                context.ReplaceChannelSource(context.Binding.GetWriteChannelSource(cancellationToken));
                context.ReplaceChannel(context.ChannelSource.GetChannel(cancellationToken));
            }
            catch
            {
                throw originalException;
            }

            if (!AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                throw originalException;
            }

           
            var attempt = 2;
            var retryExceptions = new List<Exception>();
            retryExceptions.Add(originalException);
            var maxRetries = (30 - (attempt-1));
            var retryPauseSeconds = 1;
            while (true)
            {
                try
                {
                    System.Console.WriteLine(" ***** About to retry, attempt = "+attempt+" ***** ");
                    try
                    {
                        context.ReplaceChannelSource(context.Binding.GetWriteChannelSource(cancellationToken));
                        context.ReplaceChannel(context.ChannelSource.GetChannel(cancellationToken));
                    }
                    catch (Exception exp) 
                    {
                        System.Console.WriteLine(" ***** Before retry channel replace failed. ***** ");
                        System.Console.WriteLine(exp);
                        retryExceptions.Add(exp);
                        throw new AggregateException(retryExceptions);
                    }
                    var retryResult = operation.ExecuteAttempt(context, attempt, transactionNumber, cancellationToken);
                    return retryResult;
                }
                catch (Exception ex) when (IsRetryableException(ex))
                {
                    retryExceptions.Add(ex);
                    // add throttle logic
                    System.Threading.Thread.Sleep(1000 * retryPauseSeconds);

                }
                catch (Exception ex) when (ShouldThrowOriginalException(ex))
                {
                    retryExceptions.Add(ex);
                    throw new AggregateException(retryExceptions);
                }
                attempt = attempt + 1;
                if (attempt >= maxRetries) {
                    retryExceptions.Add(new Exception("Reached max retry attempts of " + maxRetries));
                    throw new AggregateException(retryExceptions);
                }
            }
        }

        public async static Task<TResult> ExecuteAsync<TResult>(IRetryableWriteOperation<TResult> operation, IWriteBinding binding, bool retryRequested, CancellationToken cancellationToken)
        {
            using (var context = await RetryableWriteContext.CreateAsync(binding, retryRequested, cancellationToken).ConfigureAwait(false))
            {
                return await ExecuteAsync(operation, context, cancellationToken).ConfigureAwait(false);
            }
        }

        public static async Task<TResult> ExecuteAsync<TResult>(IRetryableWriteOperation<TResult> operation, RetryableWriteContext context, CancellationToken cancellationToken)
        {
            if (!context.RetryRequested || !AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                return await operation.ExecuteAttemptAsync(context, 1, null, cancellationToken).ConfigureAwait(false);
            }

            var transactionNumber = context.Binding.Session.AdvanceTransactionNumber();
            Exception originalException;
            try
            {
                return await operation.ExecuteAttemptAsync(context, 1, transactionNumber, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsRetryableException(ex))
            {
                originalException = ex;
            }

            try
            {
                context.ReplaceChannelSource(await context.Binding.GetWriteChannelSourceAsync(cancellationToken).ConfigureAwait(false));
                context.ReplaceChannel(await context.ChannelSource.GetChannelAsync(cancellationToken).ConfigureAwait(false));
            }
            catch
            {
                throw originalException;
            }

            if (!AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                throw originalException;
            }

            try
            {
                return await operation.ExecuteAttemptAsync(context, 2, transactionNumber, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ShouldThrowOriginalException(ex))
            {
                throw originalException;
            }
        }

        // privates static methods
        private static bool AreRetryableWritesSupported(ConnectionDescription connectionDescription)
        {
            var result = connectionDescription.IsMasterResult.LogicalSessionTimeout != null &&
                connectionDescription.IsMasterResult.ServerType != ServerType.Standalone;
            System.Console.WriteLine(" ****** AreRetryableWritesSupported result=" + result + " ****** ");
            return
                connectionDescription.IsMasterResult.LogicalSessionTimeout != null &&
                connectionDescription.IsMasterResult.ServerType != ServerType.Standalone;
        }

        private static bool IsRetryableException(Exception ex)
        {
            var retryableError = false;
            if (ex is MongoCommandException) {
                retryableError = IsRetryableExceptionErrorCode((MongoCommandException)ex);
                System.Console.WriteLine("***** retryableError=" + retryableError + " *****");
            }
            return
                ex is MongoConnectionException ||
                ex is MongoNotPrimaryException ||
                ex is MongoNodeIsRecoveringException ||
                retryableError;
        }

        private static Dictionary<int, string> retryableErrorCodes =
            new Dictionary<int, string>() {

            { 11600, "InterruptedAtShutdown"}
            ,{ 11602, "InterruptedDueToReplStateChange" }
            ,{ 10107, "NotMaster" }
            ,{ 13435, "NotMasterNoSlaveOk" }
            ,{ 13436, "NotMasterOrSecondary" }
            ,{ 189, "PrimarySteppedDown" }
            ,{ 91, "ShutdownInProgress" }
            ,{ 64, "WriteConcernFailed" }
            ,{ 7, "HostNotFound" }
            ,{ 6, "HostUnreachable" }
            ,{ 89, "NetworkTimeout" }
            ,{ 9001, "SocketException" }
        };


        private static bool IsRetryableExceptionErrorCode(MongoCommandException ex) 
        {
            // not sure how to log here....
            System.Console.WriteLine(" ***** IsRetryableExceptionErrorCode Code=" + ex.Code + " *****");
            return retryableErrorCodes.ContainsKey(ex.Code);
            
        }
        private static bool ShouldThrowOriginalException(Exception retryException)
        {
            var ret = retryException is MongoException && !(retryException is MongoConnectionException);
            System.Console.WriteLine(" ***** ShouldThrowOriginalException return:" + ret);
            //return retryException is MongoException && !(retryException is MongoConnectionException);
            return ret;
        }
    }
}
