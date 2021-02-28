package com.iiichz.embeddedhbase;

import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import org.easymock.internal.ClassInstantiatorFactory;

class ClassProxy {
    private ClassProxy() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(Class<?> klass, MethodInterceptor handler)
            throws InstantiationException {
        // Don't ask me how this work...
        final Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(klass);
        enhancer.setInterceptDuringConstruction(true);
        enhancer.setCallbackType(handler.getClass());
        final Class<?> proxyClass = enhancer.createClass();
        final Factory proxy =
                (Factory) ClassInstantiatorFactory.getInstantiator().newInstance(proxyClass);
        proxy.setCallbacks(new Callback[] { handler });
        return (T) proxy;
    }
}
