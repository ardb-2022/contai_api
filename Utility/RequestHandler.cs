using Microsoft.AspNetCore.Http;

public interface IRequestContext
{
    string BankName { get; }
}

public sealed class RequestContextAdapter : IRequestContext
{
    private readonly HttpContext _accessor;
    public RequestContextAdapter(HttpContext accessor)
    {
        this._accessor = accessor;
    }

    public string BankName
    {
        get
        {
            return this._accessor.Request.Headers["bname"];
        }
    }
}