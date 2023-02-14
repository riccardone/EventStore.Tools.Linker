namespace Linker;

public interface IJsonService
{
    T Deserialise<T>(string data);
    string Serialise(object data);
}