namespace Linker;

public class LinkerPosition : ILinkerPosition
{
    public static readonly LinkerPosition Start = new(0uL, 0uL);
    public static readonly LinkerPosition End = new(ulong.MaxValue, ulong.MaxValue);
    public readonly ulong CommitPosition;
    public readonly ulong PreparePosition;

    public LinkerPosition(ulong commitPosition, ulong preparePosition)
    {
        if (commitPosition < preparePosition)
            throw new ArgumentException("The commit position cannot be less than the prepare position", "commitPosition");
        CommitPosition = commitPosition;
        PreparePosition = preparePosition;
    }
}