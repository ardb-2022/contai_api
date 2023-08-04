using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Logger;
using System.Net;

namespace SBWSFinanceApi.Extensions
{
    public static class ExceptionMiddlewareExtensions
    {
        public static void ConfigureExceptionHandler(this IApplicationBuilder app)
        {
            app.UseExceptionHandler(appError =>
            {
                appError.Run(async context =>
                {
                    context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    context.Response.ContentType = "application/json";

                    ExceptionHandlerFeature contextFeature = (ExceptionHandlerFeature)context.Features.Get<IExceptionHandlerFeature>();
                    if(null != contextFeature  &&
                    null != contextFeature.Error)
                    {
                        JsonFileLogger logger = new JsonFileLogger();
                        logger.Log(new Log{
                            ApiName = contextFeature.Path,
                            Message = contextFeature.Error.Message,
                            OccurTimeStamp = System.DateTime.Now.ToString()
                        });
                        // logger.LogError($"Something went wrong: {contextFeature.Error}");

                        await context.Response.WriteAsync(new ErrorDetails()
                        {
                            ApiName = contextFeature.Path,
                            StatusCode = context.Response.StatusCode,
                            Message = contextFeature.Error.Message,
                            // StackTrace = contextFeature.Error.StackTrace
                        }.ToString());
                    }
                });
            });
        }
    }
}