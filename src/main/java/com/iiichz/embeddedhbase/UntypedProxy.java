package com.iiichz.embeddedhbase;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.InvocationTargetException;

public class UntypedProxy<T> implements MethodInterceptor {

    private final T mTarget;

    public static <T, U> T create(Class<T> qlass, U handler) throws InstantiationException {
        return ClassProxy.create(qlass, new UntypedProxy<U>(handler));
    }


    public UntypedProxy(T target) {
        mTarget = target;
    }

    @Override
    public Object intercept(Object self, java.lang.reflect.Method method, Object[] args, MethodProxy proxy) throws Throwable {
        try {
            return mTarget.getClass().getMethod(method.getName(), method.getParameterTypes()).invoke(mTarget, args);
        } catch (InvocationTargetException ite) {
            throw ite.getCause();
        }
    }
}
