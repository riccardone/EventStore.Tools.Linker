namespace Linker;

public class Position
{
    public readonly long CommitPosition;

    public readonly long PreparePosition;

    public Position(long commitPosition, long preparePosition)
    {
        if (commitPosition < preparePosition)
            throw new ArgumentException("The commit position cannot be less than the prepare position", "commitPosition");
        CommitPosition = commitPosition;
        PreparePosition = preparePosition;
    }
}