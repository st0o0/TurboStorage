namespace NaschStorage.UnitTests;

public sealed class BlobItemTests
{
    [Fact]
    public void BlobItem_RequiredPath_IsSet()
    {
        var item = new BlobItem { Path = "/test/file.txt" };
        Assert.Equal("/test/file.txt", item.Path);
        Assert.Equal(BlobKind.File, item.Kind);
        Assert.Null(item.Size);
        Assert.Null(item.CreatedOn);
        Assert.Null(item.ModifiedOn);
        Assert.Null(item.ContentType);
        Assert.Null(item.ETag);
        Assert.Null(item.Properties);
    }

    [Fact]
    public void BlobItem_WithAllProperties_RoundTrips()
    {
        var props = new Dictionary<string, string> { ["key"] = "value" };
        var now = DateTimeOffset.UtcNow;
        var item = new BlobItem
        {
            Path = "/folder/doc.pdf",
            Kind = BlobKind.File,
            Size = 1024,
            CreatedOn = now,
            ModifiedOn = now,
            ContentType = "application/pdf",
            ETag = "\"abc123\"",
            Properties = props,
        };
        Assert.Equal("/folder/doc.pdf", item.Path);
        Assert.Equal(BlobKind.File, item.Kind);
        Assert.Equal(1024, item.Size);
        Assert.Equal(now, item.CreatedOn);
        Assert.Equal(now, item.ModifiedOn);
        Assert.Equal("application/pdf", item.ContentType);
        Assert.Equal("\"abc123\"", item.ETag);
        Assert.Equal("value", item.Properties!["key"]);
    }

    [Fact]
    public void BlobItem_Folder_HasCorrectKind()
    {
        var item = new BlobItem { Path = "/my-folder", Kind = BlobKind.Folder };
        Assert.Equal(BlobKind.Folder, item.Kind);
    }

    [Fact]
    public void BlobItem_IsImmutable_WithExpression()
    {
        var original = new BlobItem { Path = "/a.txt", Size = 100 };
        var modified = original with { Size = 200 };
        Assert.Equal(100, original.Size);
        Assert.Equal(200, modified.Size);
        Assert.Equal(original.Path, modified.Path);
    }
}
