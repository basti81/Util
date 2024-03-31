package backend.core.lib.commons.stream;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.jms.JmsException;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import backend.core.lib.commons.jms.JmsMessage;
import backend.core.lib.commons.jms.messages.JmsMessageSession;
import backend.core.lib.commons.jms.messages.JmsStreamMessageExercise;
import backend.core.lib.commons.jms.messages.JmsStreamMessageJdnUnit;
import backend.core.lib.commons.jms.messages.JmsStreamMessageTeam;
import backend.core.lib.commons.jms.messages.JmsStreamMessageTeamUnits;
import backend.core.lib.commons.jms.messages.JmsStreamMessageTeamUser;
import backend.core.lib.commons.jms.messages.JmsStreamMessageTeamUsers;
import backend.core.lib.commons.jms.messages.JmsStreamMessageUnit;
import backend.core.lib.commons.jms.messages.JmsStreamMessageUnits;
import backend.core.lib.commons.jms.messages.JmsStreamMessageUserConnected;
import backend.core.lib.commons.jms.messages.JmsStreamMessageUsers;
import backend.core.lib.commons.stream.ot.OTConectado;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEventService implements IStreamDisconnect
{

    private static final String NO_HAY_CLLIENTES_CONECTADOS = "No hay cllientes conectados";

    @Autowired
    public ObjectMapper mapper;

    @Value("${sisdef.activemq.enable:false}")
    public boolean activeMQEnable;

    @Autowired
    private JmsTemplate jmsTemplate;

    protected final List<MarteSseClient> emitters = Collections.synchronizedList(new ArrayList<>());

    protected final Long instanceId = Instant.now().toEpochMilli();

    @PostConstruct
    private void init()
    {
        log.info("[STREAM] Servicio inicializado con exito {} de ActiveMQ",
                activeMQEnable ? "conectado " : "desconectado");
    }

    public MarteSseClient register(MarteSseClient emitter)
    {
        log.info("Registrando nuevo cliente stream {}", emitter.toString(), emitter.getTimeout());
        emitters.add(emitter);
        emitter.onError(e -> {
            emitter.complete();
            emitters.remove(emitter);
            sendDisconnect(emitter);

        });
        emitter.onTimeout(() -> {
            emitter.complete();
            emitters.remove(emitter);
            sendDisconnect(emitter);

        });
        emitter.onCompletion(() -> {
            emitters.remove(emitter);
            log.debug("Removiendo client x  finalizacion de la conexion  {}", emitter.toString());
            sendDisconnect(emitter);

        });
        try
        {
            MessageSendToClient ctr = MessageSendToClient.builder().modulo(emitter.modulo)
                    .operacion(EnumOperacion.CONNECTED).build();
            emitter.send(ctr, MediaType.APPLICATION_JSON);

            try
            {
                JmsStreamMessageUserConnected jms = new JmsStreamMessageUserConnected();
                jms.setInstance(instanceId);
                jms.setModulo(emitter.modulo);
                jms.setOperacion(EnumOperacion.CONNECTED);
                log.debug("Enviando a topico {} el mensaje JMS {}", emitter.modulo.name(), jms);

                sendJms(emitter.modulo, mapper.writeValueAsString(jms));

            }
            catch (JmsException e)
            {
                log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

            }
            return emitter;
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
            return null;
        }
    }

    /**
     * Envia emnsaje a equipo en modulo chat, para conectado o desconectado.
     * @param emitter
     */
    private void sendDisconnect(MarteSseClient emitter)
    {
        disconnected(emitter);
        if (EnumModulo.CHAT.equals(emitter.modulo))
        {
            OTConectado otMensje = OTConectado.builder().conectado(false).idEjercicio(emitter.getIdEjercicio())
                    .idEquipo(emitter.getIdEquipo()).idUsuario(emitter.getIdPerfilUsuario()).build();
            this.mensajeEquipo(EnumModulo.CHAT, EnumOperacion.UPDATE, emitter.idEquipo, OTConectado.class, otMensje);
        }
    }

    public <T> void mensajeUsuarios(EnumModulo modulo, EnumOperacion operacion, List<Long> idperfilUsuario,
            Class<T> clazz, T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoUsuarios(modulo, operacion, idperfilUsuario, clazz.getSimpleName(), messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje a usuarios ", e);
        }
    }

    private <T> void mensajeJmsUsuarios(EnumModulo modulo, EnumOperacion operacion, List<Long> idperfilUsuario,
            String clazz, String input, boolean replicate)
    {
        mensajeInternoUsuarios(modulo, operacion, idperfilUsuario, clazz, input, replicate);
    }

    /**
     * Envia mensajes a la lista de ids de perfil de usuario
     * @param <T>
     * @param operacion
     * @param idEquipo
     * @param idperfilUsuario
     * @param clazz
     * @param input
     */
    private void mensajeInternoUsuarios(EnumModulo modulo, EnumOperacion operacion, List<Long> idperfilUsuario,
            String clazz, String input, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje a usuarios %s", input.toString()));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream()
                    .filter(e -> e.getModulo().equals(modulo) && idperfilUsuario.contains(e.getIdPerfilUsuario()))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug("Mensaje enviado al usuario [{}]", emitter.getIdPerfilUsuario());

                        }
                        catch (IOException e)
                        {
                            log.warn("error enviando mensaje a usuarios , al usuario {} error:{}",
                                    emitter.idPerfilUsuario, e.getCause().getMessage());
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageUsers jms = new JmsStreamMessageUsers();
                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setIds(idperfilUsuario);
                    jms.setClazz(clazz);
                    log.debug("Enviando a topico {} el mensaje JMS ", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    /**
     * 
     * @param <T>
     * @param modulo
     *            Modulo {@link EnumModulo}
     * @param operacion
     *            OPeracion {@link EnumOperacion}
     * @param idEquipo
     *            id de equipo
     * @param clazz
     *            clase del objeto a enviar
     * @param input
     *            Objecto a enviar
     */
    public <T> void mensajeEquipo(EnumModulo modulo, EnumOperacion operacion, Long idEquipo, String clazz, T input)

    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoEquipo(modulo, operacion, idEquipo, clazz, messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje a usuarios ", e);
        }
    }

    private void mensajeJmsEquipo(EnumModulo modulo, EnumOperacion operacion, Long idEquipo, String clazz, String input,
            boolean replicate)
    {
        mensajeInternoEquipo(modulo, operacion, idEquipo, clazz, input, replicate);

    }

    public <T> void mensajeEquipo(EnumModulo modulo, EnumOperacion operacion, Long idEquipo, Class<T> clazz, T input)
    {
        mensajeEquipo(modulo, operacion, idEquipo, clazz.getSimpleName(), input);
    }

    /**
     * Envia mensajes a Equipo
     * @param <T>
     * @param modulo
     * @param operacion
     * @param idEquipo
     * @param clazz
     * @param input
     */
    public void mensajeInternoEquipo(EnumModulo modulo, EnumOperacion operacion, Long idEquipo, String clazz,
            String input, boolean replicate)
    {
        try
        {
            // String messageString = mapper.writeValueAsString(input);
            log.debug("Enviando mensaje a equipo {} {}", idEquipo, input.toString());

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream()
                    .filter(e -> e.getModulo().equals(modulo) && Objects.equals(e.getIdEquipo(), idEquipo))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug("Mensaje enviado al equipo {} al usuario {}", idEquipo,
                                    emitter.getIdPerfilUsuario());

                        }
                        catch (IOException e)
                        {
                            log.warn("error enviando mensaje equipo al equipo {} al usuario: {} error:{}", idEquipo,
                                    emitter.getIdPerfilUsuario(), e.getCause().getMessage());
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageTeam jms = new JmsStreamMessageTeam();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setTeamId(idEquipo);
                    jms.setClazz(clazz);

                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeEjercicio(EnumModulo modulo, EnumOperacion operacion, Long idEjercicio, Class<T> clazz,
            T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoEjercicio(modulo, operacion, idEjercicio, clazz.getSimpleName(), messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje a usuarios ", e);
        }
    }

    private void mensajeJmsEjercicio(EnumModulo modulo, EnumOperacion operacion, Long idEjercicio, String clazz,
            String input, boolean replicate)
    {
        mensajeInternoEjercicio(modulo, operacion, idEjercicio, clazz, input, replicate);

    }

    /**
     * Envia mensajes a Equipo
     * @param <T>
     * @param modulo
     * @param operacion
     * @param idEquipo
     * @param clazz
     * @param input
     */
    private void mensajeInternoEjercicio(EnumModulo modulo, EnumOperacion operacion, Long idEjercicio, String clazz,
            String input, boolean replicate)
    {
        try
        {
            log.debug("Enviando mensaje a ejercicio :{}", input);

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream()
                    .filter(e -> e.getModulo().equals(modulo) && Objects.equals(e.getIdEjercicio(), idEjercicio))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje a Ejercicio  enviado al cliente [%s]",
                                    emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.warn(String.format("error enviando mensaje a Ejercicio al cliente [%s]",
                                    e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageExercise jms = new JmsStreamMessageExercise();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setExerciseId(idEjercicio);
                    jms.setClazz(clazz);

                    log.debug("Enviando a topico {} el mensaje JMS ", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeUnidad(EnumModulo modulo, EnumOperacion operacion, Long idUnidad, Class<T> clazz, T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoUnidad(modulo, operacion, idUnidad, clazz.getSimpleName(), messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje a usuarios ", e);
        }
    }

    private void mensajeJmsUnidad(EnumModulo modulo, EnumOperacion operacion, Long idUnidad, String clazz, String input,
            boolean replicate)
    {
        mensajeInternoUnidad(modulo, operacion, idUnidad, clazz, input, replicate);

    }

    /**
     * Envia mensajes a usuarios conectados de la unidad
     * @param <T>
     * @param modulo
     * @param operacion
     * @param idEquipo
     * @param clazz
     * @param input
     */
    private void mensajeInternoUnidad(EnumModulo modulo, EnumOperacion operacion, Long idUnidad, String clazz,
            String input, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje a unidad  %s", input.toString()));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream()
                    .filter(e -> e.getModulo().equals(modulo) && Objects.equals(e.getIdUnidad(), idUnidad))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje a Unidad enviado al cliente [%s]",
                                    emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.warn(String.format("error enviando mensaje a unidad  al cliente [%s]",
                                    e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageUnit jms = new JmsStreamMessageUnit();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setUnitId(idUnidad);
                    jms.setClazz(clazz);

                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeUnidadJDN(EnumModulo modulo, EnumOperacion operacion, Long idUnidadJdn, Class<T> clazz,
            T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoUnidadJDN(modulo, operacion, idUnidadJdn, clazz.getSimpleName(), messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje a usuarios ", e);
        }
    }

    private void mensajeJmsUnidadJDN(EnumModulo modulo, EnumOperacion operacion, Long idUnidadJdn, String clazz,
            String input, boolean replicate)
    {
        mensajeInternoUnidadJDN(modulo, operacion, idUnidadJdn, clazz, input, replicate);

    }

    /**
     * Envia mensajes a usuarios conectados de la unidad JDN
     * @param <T>
     * @param modulo
     * @param operacion
     * @param idEquipo
     * @param clazz
     * @param input
     */
    private void mensajeInternoUnidadJDN(EnumModulo modulo, EnumOperacion operacion, Long idUnidadJdn, String clazz,
            String input, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje unidad JDN %s", input.toString()));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream()
                    .filter(e -> e.getModulo().equals(modulo) && Objects.equals(e.getIdUnidadJdn(), idUnidadJdn))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje enviado a unidad JDN al cliente [%s]",
                                    emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.warn(String.format("error enviando mensaje a unidad JDN al cliente [%s]",
                                    e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageJdnUnit jms = new JmsStreamMessageJdnUnit();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setUnitId(idUnidadJdn);

                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeEquipoUsuarios(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idperfilUsuario, Class<T> clazz, T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoEquipoUsuarios(modulo, operacion, idEquipo, idperfilUsuario, clazz.getSimpleName(),
                    messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje", e);
        }
    }

    private void mensajeJmsEquipoUsuarios(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idperfilUsuario, String clazz, String input, boolean replicate)
    {
        mensajeInternoEquipoUsuarios(modulo, operacion, idEquipo, idperfilUsuario, clazz, input, replicate);
    }

    /**
     * Mensaje a usuarios que cumplan idEquipo, Lista de idPerfilUsuario
     * @param <T>
     * @param operacion
     * @param idEquipo
     * @param idperfilUsuario
     * @param clazz
     * @param input
     */
    private void mensajeInternoEquipoUsuarios(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idperfilUsuario, String clazz, String input, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje a usuarios de un equipo %s", input.toString()));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream().filter(e -> e.getModulo().equals(modulo)
                    && Objects.equals(e.getIdEquipo(), idEquipo) && idperfilUsuario.contains(e.getIdPerfilUsuario()))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje enviado usuarios de un equipo al cliente [%s]",
                                    emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.debug(String.format("error enviando mensaje a usuarios de un equipo al cliente [%s]",
                                    e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageTeamUsers jms = new JmsStreamMessageTeamUsers();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setTeamId(idEquipo);
                    jms.setUsersId(idperfilUsuario);
                    jms.setClazz(clazz);
                    String jsonJms = mapper.writeValueAsString(jms);

                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jsonJms);
                    sendJms(modulo, jsonJms);
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeEquipoUsuario(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            Long idperfilUsuario, Class<T> clazz, T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoEquipoUsuario(modulo, operacion, idEquipo, idperfilUsuario, clazz.getSimpleName(),
                    messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje", e);
        }
    }

    private void mensajeJmsEquipoUsuario(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            Long idperfilUsuario, String clazz, String input, boolean replicate)
    {
        mensajeInternoEquipoUsuario(modulo, operacion, idEquipo, idperfilUsuario, clazz, input, replicate);
    }

    /**
     * Mensaje a usuarios que cumplan idEquipo, Lista de idPerfilUsuario
     * @param <T>
     * @param operacion
     * @param idEquipo
     * @param idperfilUsuario
     * @param clazz
     * @param input
     */
    private void mensajeInternoEquipoUsuario(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            Long idperfilUsuario, String clazz, String input, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje a usuarios de un equipo %s", input.toString()));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream()
                    .filter(e -> e.getModulo().equals(modulo) && Objects.equals(e.getIdEquipo(), idEquipo)
                            && Objects.equals(idperfilUsuario, e.getIdPerfilUsuario()))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje enviado usuarios de un equipo al cliente [%s]",
                                    emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.debug(String.format("error enviando mensaje a usuarios de un equipo al cliente [%s]",
                                    e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageTeamUser jms = new JmsStreamMessageTeamUser();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setTeamId(idEquipo);
                    jms.setClazz(clazz);

                    jms.setUserId(idperfilUsuario);
                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeUnidades(EnumModulo modulo, EnumOperacion operacion, List<Long> idUnidad, Class<T> clazz,
            List<T> inputs)
    {
        try
        {
            String messageString = mapper.writeValueAsString(inputs);
            mensajeInternoUnidades(modulo, operacion, idUnidad, clazz.getSimpleName(), messageString, false);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje", e);
        }
    }

    public <T> void mensajeUnidades(EnumModulo modulo, EnumOperacion operacion, List<Long> idUnidad, Class<T> clazz,
            T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoUnidades(modulo, operacion, idUnidad, clazz.getSimpleName(), messageString, true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje", e);
        }
    }

    private void mensajeJmsUnidades(EnumModulo modulo, EnumOperacion operacion, List<Long> idUnidad, String clazz,
            String input, boolean replicate)
    {
        mensajeInternoUnidades(modulo, operacion, idUnidad, clazz, input, replicate);
    }

    /**
     * Envia mensajes a usuarios conectados a las unidades
     * @param <T>
     * @param modulo
     * @param operacion
     * @param idEquipo
     * @param clazz
     * @param input
     */
    private void mensajeInternoUnidades(EnumModulo modulo, EnumOperacion operacion, List<Long> idUnidad, String clazz,
            String input, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje %s", input.toString()));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream().filter(e -> e.getModulo().equals(modulo) && idUnidad.contains(e.getIdUnidad()))
                    .forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(input).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje enviado al cliente [%s]", emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.warn(String.format("error enviando datos al cliente [%s]", e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageUnits jms = new JmsStreamMessageUnits();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(input);
                    jms.setUnitId(idUnidad);
                    jms.setClazz(clazz);

                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));
                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    public <T> void mensajeEquipoUnidades(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idsUnidad, Class<T> clazz, List<T> input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoEquipoUnidades(modulo, operacion, idEquipo, idsUnidad, clazz.getSimpleName(), messageString,
                    true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje", e);
        }
    }

    public <T> void mensajeEquipoUnidades(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idsUnidad, Class<T> clazz, T input)
    {
        try
        {
            String messageString = mapper.writeValueAsString(input);
            mensajeInternoEquipoUnidades(modulo, operacion, idEquipo, idsUnidad, clazz.getSimpleName(), messageString,
                    true);
        }
        catch (Exception e)
        {
            log.error("Error al enviar mensaje", e);
        }
    }

    private void mensajeJmsEquipoUnidades(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idsUnidad, String clazz, String input, boolean replicate)
    {
        mensajeInternoEquipoUnidades(modulo, operacion, idEquipo, idsUnidad, clazz, input, replicate);

    }

    /**
     * Envia mensajes a usuarios conectados a las unidades y equipos
     * @param <T>
     * @param modulo
     * @param operacion
     * @param idEquipo
     * @param clazz
     * @param input
     */
    private void mensajeInternoEquipoUnidades(EnumModulo modulo, EnumOperacion operacion, Long idEquipo,
            List<Long> idsUnidad, String clazz, String messageString, boolean replicate)
    {
        try
        {
            log.debug(String.format("Enviando mensaje %s", messageString));

            if (emitters.isEmpty())
            {
                log.debug(NO_HAY_CLLIENTES_CONECTADOS);
            }
            List<MarteSseClient> procesar = new ArrayList<>(emitters);
            procesar.parallelStream().filter(e -> e.getModulo().equals(modulo) && e.getIdEquipo().equals(idEquipo)
                    && idsUnidad.contains(e.getIdUnidad())).forEach(emitter -> {
                        try
                        {
                            MessageSendToClient msg = MessageSendToClient.builder().modulo(modulo).operacion(operacion)
                                    .data(messageString).clazz(clazz).build();
                            emitter.send(msg, MediaType.APPLICATION_JSON);
                            log.debug(String.format("Mensaje enviado al cliente [%s]", emitter.getIdPerfilUsuario()));

                        }
                        catch (IOException e)
                        {
                            log.warn(String.format("error enviando datos al cliente [%s]", e.getCause().getMessage()));
                            emitter.completeWithError(e);
                        }
                    });

            if (replicate)
            {
                try
                {
                    JmsStreamMessageTeamUnits jms = new JmsStreamMessageTeamUnits();
                    jms.setInstance(instanceId);

                    jms.setModulo(modulo);
                    jms.setOperacion(operacion);
                    jms.setMessage(messageString);
                    jms.setUnitId(idsUnidad);
                    jms.setTeamId(idEquipo);
                    jms.setClazz(clazz);

                    log.debug("Enviando a topico {} el mensaje JMS {}", modulo.name(), jms);
                    sendJms(modulo, mapper.writeValueAsString(jms));

                }
                catch (JmsException e)
                {
                    log.error("Problema al enviar mensaje por JMS a ActiveMQ", e);

                }
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }

    protected void processJmsMessage(String message)
    {
        try
        {
            JmsMessage mensaje = mapper.readValue(message, JmsMessage.class);
            if (mensaje.getInstance() == instanceId)
            {
                return;// No se procesa por que es originado por esta misma instancia
            }

            switch (mensaje.getTipo())
            {
                case MENSAJE_CONECTADO:
                    // JmsStreamMessageUserConnected msg = mapper.readValue(message,
                    // JmsStreamMessageUserConnected.class);
                    break;
                case MENSAJE_EQUIPO:
                    JmsStreamMessageTeam msg1 = mapper.readValue(message, JmsStreamMessageTeam.class);
                    mensajeJmsEquipo(msg1.getModulo(), msg1.getOperacion(), msg1.getTeamId(), msg1.getClazz(),
                            msg1.getMessage(), false);
                    break;
                case MENSAJE_EJERCICIO:
                    JmsStreamMessageExercise msg2 = mapper.readValue(message, JmsStreamMessageExercise.class);
                    mensajeJmsEjercicio(msg2.getModulo(), msg2.getOperacion(), msg2.getExerciseId(), msg2.getClazz(),
                            msg2.getMessage(), false);
                    break;
                case MENSAJE_EQUIPO_UNIDADES:
                    JmsStreamMessageTeamUnits msg3 = mapper.readValue(message, JmsStreamMessageTeamUnits.class);
                    mensajeJmsEquipoUnidades(msg3.getModulo(), msg3.getOperacion(), msg3.getTeamId(), msg3.getUnitId(),
                            msg3.getClazz(), msg3.getMessage(), false);

                    break;
                case MENSAJE_EQUIPO_USUARIO:
                    JmsStreamMessageTeamUser msg4 = mapper.readValue(message, JmsStreamMessageTeamUser.class);
                    mensajeJmsEquipoUsuario(msg4.getModulo(), msg4.getOperacion(), msg4.getTeamId(), msg4.getUserId(),
                            msg4.getClazz(), msg4.getMessage(), false);
                    break;
                case MENSAJE_EQUIPO_USUARIOS:
                    JmsStreamMessageTeamUsers msg5 = mapper.readValue(message, JmsStreamMessageTeamUsers.class);
                    mensajeJmsEquipoUsuarios(msg5.getModulo(), msg5.getOperacion(), msg5.getTeamId(), msg5.getUsersId(),
                            msg5.getClazz(), msg5.getMessage(), false);
                    break;
                case MENSAJE_UNIDAD:
                    JmsStreamMessageUnit msg6 = mapper.readValue(message, JmsStreamMessageUnit.class);
                    mensajeJmsUnidad(msg6.getModulo(), msg6.getOperacion(), msg6.getUnitId(), msg6.getClazz(),
                            msg6.getMessage(), false);
                    break;
                case MENSAJE_UNIDAD_JDN:
                    JmsStreamMessageJdnUnit msg7 = mapper.readValue(message, JmsStreamMessageJdnUnit.class);
                    mensajeJmsUnidadJDN(msg7.getModulo(), msg7.getOperacion(), msg7.getUnitId(), msg7.getClazz(),
                            msg7.getMessage(), false);
                    break;
                case MENSAJE_UNIDADES:
                    JmsStreamMessageUnits msg8 = mapper.readValue(message, JmsStreamMessageUnits.class);
                    mensajeJmsUnidades(msg8.getModulo(), msg8.getOperacion(), msg8.getUnitId(), msg8.getClazz(),
                            msg8.getMessage(), false);
                    break;
                case MENSAJE_USUARIOS:
                    JmsStreamMessageUsers msg9 = mapper.readValue(message, JmsStreamMessageUsers.class);
                    mensajeJmsUsuarios(msg9.getModulo(), msg9.getOperacion(), msg9.getIds(), msg9.getClazz(),
                            msg9.getMessage(), false);
                    break;

                default:
                    log.warn("No se pudo procesar el mensaje {}", message);
                    break;
            }
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
        }
    }

    private void sendJms(EnumModulo topico, String mensaje)
    {
        if (activeMQEnable)
        {
            log.debug("[ActiveMQ] Enviando al topico {} el mensaje:{}", topico.name(), mensaje);
            jmsTemplate.convertAndSend(topico.name(), mensaje);
        }

    }

    public List<MarteSseClient> getEmitters()
    {
        return emitters;

    }

    protected void removeSession(JmsMessageSession mensaje)
    {
        if (emitters.isEmpty())
        {
            log.debug(NO_HAY_CLLIENTES_CONECTADOS);
        }

        List<MarteSseClient> analizeList = new ArrayList<>(emitters);

//        for (MarteSseClient cliente : analizeList)
//        {
//            log.debug("[STREAM-JMS] analize "+cliente);
//        }

        List<MarteSseClient> closeSessions = analizeList.stream()
                .filter(c -> c.getIdClientSession().equals(mensaje.getIdClientSession())).collect(Collectors.toList());
        for (MarteSseClient sseClient : closeSessions)
        {
            try
            {
                log.debug("[STREAM-JMS] Remove Session  {}", sseClient);

                sseClient.complete();

            }
            catch (Throwable e)
            {
                sseClient.completeWithError(e);
            }

        }

    }

    @JmsListener(destination = "SESSION", concurrency = "1-1")
    public void sessionClose(String strMessage)
    {
        try
        {
            log.debug("[STREAM-JMS] {} Recibiendo mensaje d session desde jms {}", instanceId, strMessage);
            JmsMessageSession mensaje = mapper.readValue(strMessage, JmsMessageSession.class);
            if (!mensaje.isConnected())
                removeSession(mensaje);

        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }

    }

}
