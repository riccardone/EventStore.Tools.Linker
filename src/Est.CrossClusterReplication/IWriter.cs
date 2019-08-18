namespace Est.CrossClusterReplication
{
    public interface IWriter<in T>
    {
        void Set(T item);
    }
}
