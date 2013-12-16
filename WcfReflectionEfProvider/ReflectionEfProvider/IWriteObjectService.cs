namespace ReflectionEfProvider
{
    public interface IWriteObjectService
    {
        void Create(object obj);
        void Update(object obj);
        void Delete(object obj);
    }
}