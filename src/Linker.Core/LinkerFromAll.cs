namespace Linker.Core;

public readonly struct LinkerFromAll 
{
    public static readonly LinkerFromAll Start = new(null);
    public static readonly LinkerFromAll End = new(LinkerPosition.End);
    private readonly LinkerPosition? _value;

    private LinkerFromAll(LinkerPosition? value)
    {
        _value = value;
    }
}