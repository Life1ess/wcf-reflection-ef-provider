using System.Linq;

namespace ReflectionEfProvider
{
    public interface IReadObjectService
    {
        bool ProxyCreationEnabled { set; get; }
        IQueryable GetAll();
        object GetById(object obj);
    }
}