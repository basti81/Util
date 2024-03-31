package backend.core.lib.commons.stream;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEventController
{
    /**
     * @param ppal
     * @return
     */
    protected MarteSseClient configureSseClient(StreamConfig ctx, EnumModulo modulo)
    {
        Long idEquipo = ctx.getIdEquipo();
        Long idUsuario = ctx.getIdUsuario();
        Long idEjercicio = ctx.getIdEjercicio();
        Long idClientSession = ctx.getIdClientSession();
        Long idUnidad = null;
        if (ctx.getIdUnidad() != null)
            idUnidad = ctx.getIdUnidad();
        Long idUnidadJdn = null;
        if (ctx.getIdUnidadJdn() != null)
            idUnidadJdn = ctx.getIdUnidad();
        

        log.debug("Conectando a stream  ",ctx.toString());
        return new MarteSseClient(modulo, idEquipo, idUsuario, idUnidad, idUnidadJdn, idEjercicio, idClientSession);
    }
}
