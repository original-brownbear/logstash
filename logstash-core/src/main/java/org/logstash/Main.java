package org.logstash;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jruby.Ruby;
import org.jruby.RubyException;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyNumeric;
import org.jruby.exceptions.JumpException;
import org.jruby.exceptions.MainExitException;
import org.jruby.exceptions.RaiseException;
import org.jruby.exceptions.ThreadKill;
import org.jruby.platform.Platform;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.SafePropertyAccessor;
import org.jruby.util.cli.OutputStrings;
import org.jruby.util.log.Logger;
import org.jruby.util.log.LoggerFactory;

/**
 * Class used to launch the interpreter.
 * This is the main class as defined in the jruby.mf manifest.
 * It is very basic and does not support yet the same array of switches
 * as the C interpreter.
 * Usage: java -jar jruby.jar [switches] [rubyfile.rb] [arguments]
 * -e 'command'    one line of script. Several -e's allowed. Omit [programfile]
 * @author jpetersen
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main(final boolean hardExit) {
        // used only from main(String[]), so we process dotfile here
        processDotfile();
        this.config = new RubyInstanceConfig();
        config.setHardExit(hardExit);
    }

    private static Iterable<String> getDotfileDirectories() {
        final List<String> searchList = new ArrayList<>(4);
        for (final String homeProp : new String[]{"user.dir", "user.home"}) {
            final String home = SafePropertyAccessor.getProperty(homeProp);
            if (home != null) {
                searchList.add(home);
            }
        }
        // JVM sometimes picks odd location for user.home based on a registry entry
        // (see http://bugs.sun.com/view_bug.do?bug_id=4787931).  Add extra check in
        // case that entry is wrong. Add before user.home in search list.
        if (Platform.IS_WINDOWS) {
            final String homeDrive = System.getenv("HOMEDRIVE");
            final String homePath = System.getenv("HOMEPATH");
            if (homeDrive != null && homePath != null) {
                searchList.add(1, (homeDrive + homePath).replace('\\', '/'));
            }
        }
        return searchList;
    }

    private static void processDotfile() {
        final StringBuilder path = new StringBuilder();
        for (final String home : getDotfileDirectories()) {
            path.setLength(0);
            path.append(home).append("/.jrubyrc");
            final File dotfile = new File(path.toString());
            if (dotfile.exists()) {
                loadJRubyProperties(dotfile);
            }
        }
    }

    private static void loadJRubyProperties(final File dotfile) {
        FileInputStream fis = null;
        try {
            // update system properties with long form jruby properties from .jrubyrc
            final Properties sysProps = System.getProperties();
            final Properties newProps = new Properties();
            // load properties and re-set as jruby.*
            fis = new FileInputStream(dotfile);
            newProps.load(fis);
            for (final Map.Entry entry : newProps.entrySet()) {
                sysProps.put("jruby." + entry.getKey(), entry.getValue());
            }
        } catch (IOException | SecurityException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("exception loading properties from: " + dotfile, ex);
            }
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (final Exception e) {
                }
            }
        }
    }

    public static final class Status {
        private boolean isExit;
        private int status;

        /**
         * Creates a status object with the specified value and with explicit
         * exit flag. An exit flag means that Kernel.exit() has been explicitly
         * invoked during the run.
         * @param status The status value.
         */
        Status(final int status) {
            this.isExit = true;
            this.status = status;
        }

        /**
         * Creates a status object with 0 value and no explicit exit flag.
         */
        Status() {
        }

        public boolean isExit() {
            return isExit;
        }

        public int getStatus() {
            return status;
        }
    }

    /**
     * This is the command-line entry point for JRuby, and should ONLY be used by
     * Java when starting up JRuby from a command-line. Use other mechanisms when
     * embedding JRuby into another application.
     * @param args command-line args, provided by the JVM.
     */
    public static void main(final String[] args) {
        doGCJCheck();
        final Main main;
        final String[] arguments = new String[args.length + 2];
        arguments[0] = System.getenv("LOGSTASH_HOME") + "/lib/bootstrap/environment.rb";
        arguments[1] = "logstash/runner.rb";
        System.arraycopy(args, 0, arguments, 2, args.length);
        main = new Main(true);
        try {
            final Main.Status status = main.run(arguments);
            if (status.isExit()) {
                System.exit(status.getStatus());
            }
        } catch (final RaiseException ex) {
            System.exit(handleRaiseException(ex));
        } catch (final JumpException ex) {
            System.exit(handleUnexpectedJump(ex));
        } catch (Throwable t) {
            // print out as a nice Ruby backtrace
            System.err.println("Unhandled Java exception: " + t);
            System.err.println(ThreadContext.createRawBacktraceStringFromThrowable(t, false));
            while ((t = t.getCause()) != null) {
                System.err.println("Caused by:");
                System.err.println(ThreadContext.createRawBacktraceStringFromThrowable(t, false));
            }
            System.exit(1);
        }
    }

    private Main.Status run(final String[] args) {
        try {
            config.processArguments(args);
            return internalRun();
        } catch (final MainExitException mee) {
            return handleMainExit(mee);
        } catch (final StackOverflowError soe) {
            return handleStackOverflow(soe);
        } catch (final UnsupportedClassVersionError ucve) {
            return handleUnsupportedClassVersion(ucve);
        } catch (final ThreadKill kill) {
            return new Main.Status();
        }
    }

    private Main.Status internalRun() {
        doShowVersion();
        doShowCopyright();
        doPrintProperties();
        if (!config.getShouldRunInterpreter()) {
            doPrintUsage(false);
            return new Main.Status();
        }
        final InputStream in = config.getScriptSource();
        final String filename = config.displayedFileName();
        final Ruby _runtime;
        _runtime = Ruby.newInstance(config);
        final Ruby runtime = _runtime;
        final AtomicBoolean didTeardown = new AtomicBoolean();
        if (runtime != null && config.isHardExit()) {
            // we're the command-line JRuby, and should set a shutdown hook for
            // teardown.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (didTeardown.compareAndSet(false, true)) {
                    runtime.tearDown();
                }
            }));
        }
        try {
            if (runtime != null) {
                doSetContextClassLoader(runtime);
            }
            if (in == null) {
                // no script to run, return success
                return new Main.Status();
            } else if (config.isXFlag() && !config.hasShebangLine()) {
                // no shebang was found and x option is set
                throw new MainExitException(1, "jruby: no Ruby script found in input (LoadError)");
            } else if (config.getShouldCheckSyntax()) {
                // check syntax only and exit
                return doCheckSyntax(runtime, in, filename);
            } else {
                // proceed to run the script
                return doRunFromMain(runtime, in, filename);
            }
        } finally {
            if (runtime != null && didTeardown.compareAndSet(false, true)) {
                runtime.tearDown();
            }
        }
    }

    private Main.Status handleUnsupportedClassVersion(final UnsupportedClassVersionError ex) {
        config.getError()
            .println("Error: Some library (perhaps JRuby) was built with a later JVM version.");
        config.getError().println(
            "Please use libraries built with the version you intend to use or an earlier one.");
        if (config.isVerbose()) {
            ex.printStackTrace(config.getError());
        } else {
            config.getError().println("Specify -w for full " + ex + " stack trace");
        }
        return new Main.Status(1);
    }

    /**
     * Print a nicer stack size error since Rubyists aren't used to seeing this.
     */
    private Main.Status handleStackOverflow(final StackOverflowError ex) {
        final String memoryMax = getRuntimeFlagValue("-Xss");
        if (memoryMax != null) {
            config.getError().println(
                "Error: Your application used more stack memory than the safety cap of " +
                    memoryMax + '.');
        } else {
            config.getError().println(
                "Error: Your application used more stack memory than the default safety cap.");
        }
        config.getError().println("Specify -J-Xss####k to increase it (#### = cap size in KB).");
        if (config.isVerbose()) {
            ex.printStackTrace(config.getError());
        } else {
            config.getError().println("Specify -w for full " + ex + " stack trace");
        }
        return new Main.Status(1);
    }

    private static String getRuntimeFlagValue(final String prefix) {
        final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        for (final String param : runtime.getInputArguments()) {
            if (param.startsWith(prefix)) {
                return param.substring(prefix.length()).toUpperCase();
            }
        }
        return null;
    }

    private Main.Status handleMainExit(final MainExitException mee) {
        if (!mee.isAborted()) {
            config.getError().println(mee.getMessage());
            if (mee.isUsageError()) {
                doPrintUsage(true);
            }
        }
        return new Main.Status(mee.getStatus());
    }

    private static Main.Status doRunFromMain(final Ruby runtime, final InputStream in,
        final String filename) {
        try {
            doCheckSecurityManager();
            runtime.runFromMain(in, filename);
        } catch (final RaiseException rj) {
            return new Main.Status(handleRaiseException(rj));
        }
        return new Main.Status();
    }

    private Main.Status doCheckSyntax(final Ruby runtime, final InputStream in,
        final String filename) {
        // check primary script
        boolean status = checkStreamSyntax(runtime, in, filename);
        // check other scripts specified on argv
        for (final String arg : config.getArgv()) {
            status = status && checkFileSyntax(runtime, arg);
        }
        return new Main.Status(status ? 0 : -1);
    }

    private boolean checkFileSyntax(final Ruby runtime, final String filename) {
        final File file = new File(filename);
        if (file.exists()) {
            try {
                return checkStreamSyntax(runtime, new FileInputStream(file), filename);
            } catch (final FileNotFoundException ignored) {
                config.getError().println("File not found: " + filename);
                return false;
            }
        } else {
            return false;
        }
    }

    private boolean checkStreamSyntax(final Ruby runtime, final InputStream in,
        final String filename) {
        final ThreadContext context = runtime.getCurrentContext();
        final IRubyObject errors = context.getErrorInfo();
        try {
            runtime.parseFromMain(in, filename);
            config.getOutput().println("Syntax OK");
            return true;
        } catch (final RaiseException re) {
            if (re.getException().getMetaClass().getBaseName().equals("SyntaxError")) {
                context.setErrorInfo(errors);
                config.getError().println("SyntaxError in " + re.getException().message(context));
                return false;
            }
            throw re;
        }
    }

    private void doSetContextClassLoader(final Ruby runtime) {
        // set thread context JRuby classloader here, for the main thread
        try {
            Thread.currentThread().setContextClassLoader(runtime.getJRubyClassLoader());
        } catch (final SecurityException se) {
            // can't set TC classloader
            if (runtime.getInstanceConfig().isVerbose()) {
                config.getError().println(
                    "WARNING: Security restrictions disallowed setting context classloader for main thread.");
            }
        }
    }

    private void doPrintProperties() {
        if (config.getShouldPrintProperties()) {
            config.getOutput().print(OutputStrings.getPropertyHelp());
        }
    }

    private void doPrintUsage(final boolean force) {
        if (config.getShouldPrintUsage() || force) {
            config.getOutput().print(OutputStrings.getBasicUsageHelp());
            config.getOutput().print(OutputStrings.getFeaturesHelp());
        }
    }

    private void doShowCopyright() {
        if (config.isShowCopyright()) {
            config.getOutput().println(OutputStrings.getCopyrightString());
        }
    }

    private void doShowVersion() {
        if (config.isShowVersion()) {
            config.getOutput().println(OutputStrings.getVersionString());
        }
    }

    private static void doGCJCheck() {
        // Ensure we're not running on GCJ, since it's not supported and leads to weird errors
        if (Platform.IS_GCJ) {
            System.err.println("Fatal: GCJ (GNU Compiler for Java) is not supported by JRuby.");
            System.exit(1);
        }
    }

    private static void doCheckSecurityManager() {
        if (Main.class.getClassLoader() == null && System.getSecurityManager() != null) {
            System.err
                .println("Warning: security manager and JRuby running from boot classpath.\n" +
                    "Run from jruby.jar or set env VERIFY_JRUBY=true to enable security.");
        }
    }

    /**
     * This is only used from the main(String[]) path, in which case the err for this
     * run should be System.err. In order to avoid the Ruby err being closed and unable
     * to write, we use System.err unconditionally.
     */
    private static int handleRaiseException(final RaiseException ex) {
        final RubyException raisedException = ex.getException();
        final Ruby runtime = raisedException.getRuntime();
        if (runtime.getSystemExit().isInstance(raisedException)) {
            final IRubyObject status =
                raisedException.callMethod(runtime.getCurrentContext(), "status");
            if (status != null && !status.isNil()) {
                return RubyNumeric.fix2int(status);
            }
            return 0;
        }
        System.err.print(runtime.getInstanceConfig().getTraceType()
            .printBacktrace(raisedException, runtime.getPosix().isatty(FileDescriptor.err)));
        return 1;
    }

    private static int handleUnexpectedJump(final JumpException ex) {
        if (ex instanceof JumpException.SpecialJump) { // ex == JumpException.SPECIAL_JUMP
            System.err.println("Unexpected break: " + ex);
        } else if (ex instanceof JumpException.FlowControlException) {
            // NOTE: assuming a single global runtime main(args) should have :
            if (Ruby.isGlobalRuntimeReady()) {
                final Ruby runtime = Ruby.getGlobalRuntime();
                final RaiseException raise =
                    ((JumpException.FlowControlException) ex).buildException(runtime);
                if (raise != null) {
                    handleRaiseException(raise);
                }
            } else {
                System.err.println("Unexpected jump: " + ex);
            }
        } else {
            System.err.println("Unexpected: " + ex);
        }
        final StackTraceElement[] trace = ex.getStackTrace();
        if (trace != null && trace.length > 0) {
            System.err.println(ThreadContext.createRawBacktraceStringFromThrowable(ex, false));
        } else {
            System.err.println(
                "HINT: to get backtrace for jump exceptions run with -Xjump.backtrace=true");
        }
        // TODO: should match MRI (>= 2.2.3) exit status - @see ruby/test_enum.rb#test_first
        return 2;
    }

    private final RubyInstanceConfig config;
}

