namespace ReflectionEfProvider
{
    public interface IUnitOfWork
    {
        void Flush();
        void Rollback();
    }
}
