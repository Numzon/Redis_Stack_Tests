using NRedisStack;

namespace Redis_Stack_Tests.Extensions;

public static class SearchCommandsExtensions
{
    public static bool Exists(this ISearchCommands searchCommands, string indexName)
    {
        try
        {
            var info = searchCommands.Info(indexName);

            return info != null;
        }
        catch
        {
            return false;
        }
    }
}
