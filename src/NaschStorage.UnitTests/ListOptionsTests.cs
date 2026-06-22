namespace NaschStorage.UnitTests;

public sealed class ListOptionsTests
{
    [Fact]
    public void ListOptions_Defaults_AreCorrect()
    {
        var options = new ListOptions();
        Assert.Null(options.Prefix);
        Assert.False(options.Recursive);
        Assert.Null(options.MaxResults);
    }

    [Fact]
    public void ListOptions_WithValues_RoundTrips()
    {
        var options = new ListOptions
        {
            Prefix = "/data",
            Recursive = true,
            MaxResults = 50,
        };
        Assert.Equal("/data", options.Prefix);
        Assert.True(options.Recursive);
        Assert.Equal(50, options.MaxResults);
    }
}
