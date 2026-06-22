namespace NaschStorage;

public sealed record ListOptions
{
    public string? Prefix { get; init; }
    public bool Recursive { get; init; }
    public int? MaxResults { get; init; }
}
