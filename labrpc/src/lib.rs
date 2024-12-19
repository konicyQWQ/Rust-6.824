extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemTrait, ReturnType, TraitItem};

struct Method<'a> {
    name: &'a syn::Ident,
    input: Option<&'a syn::FnArg>,
    output: &'a ReturnType,
}

#[proc_macro_attribute]
pub fn labrpc(_: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemTrait);

    let trait_name = &input.ident;

    let methods: Vec<Method> = input
        .items
        .iter()
        .filter_map(|item| {
            if let TraitItem::Fn(method) = item {
                Some(Method {
                    name: &method.sig.ident,
                    input: method.sig.inputs.get(0),
                    output: &method.sig.output,
                })
            } else {
                None
            }
        })
        .collect();

    // basic struct
    let rpc_call_struct_name = format_ident!("{}RPCCall", trait_name);

    let import = quote! {
        struct #rpc_call_struct_name {
            func_name: String,
            args: String,
            sender: tokio::sync::mpsc::Sender<Option<String>>,
        }
    };

    // add &self to trait methods
    let methods_rewrite = methods.iter().map(|m| {
        let name = m.name;
        let args = m.input;
        let ret = m.output;

        let ret = match ret {
            ReturnType::Default => quote! {},
            ReturnType::Type(_, ty) => {
                quote! { -> impl std::future::Future<Output = #ty> + Send }
            }
        };

        quote! {
            fn #name(&self, #args) #ret;
        }
    });

    let trait_rewrite = quote! {
        trait #trait_name {
            #(#methods_rewrite)*
        }
    };

    // implement rpc sender
    let sender = format_ident!("{}Interface", trait_name);

    let sender_methods = methods.iter().map(|m| {
        let name = m.name;
        let args = m.input;
        let ret = m.output;

        // change args
        let args = match args {
            None => quote! { &self },
            Some(fn_args) => match fn_args {
                syn::FnArg::Receiver(_) => quote! { &self, #fn_args },
                syn::FnArg::Typed(pat_type) => {
                    let ty = &pat_type.ty;
                    quote! { &self, args: #ty }
                }
            },
        };

        // change return type
        let ret = match ret {
            ReturnType::Default => quote! {},
            ReturnType::Type(_, ty) => quote! {
                -> Result<#ty, &str>
            },
        };

        quote! {
            pub async fn #name(#args) #ret {
                let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

                self.sender
                    .send(#rpc_call_struct_name {
                        func_name: stringify!(#name).to_string(),
                        args: serde_json::to_string(&args).unwrap(),
                        sender,
                    })
                    .await
                    .expect("rpc send failed");

                let call_ret_string = receiver.recv().await.expect("rpc recv failed");

                call_ret_string
                    .map(|reply| serde_json::from_str(&reply).expect("rpc parse failed"))
                    .ok_or("Network Error")
            }
        }
    });

    let rpc_sender = quote! {
        #[derive(Clone)]
        pub struct #sender {
            sender: tokio::sync::mpsc::Sender<#rpc_call_struct_name>
        }

        impl #sender {
            #(#sender_methods)*
        }
    };

    // implement rpc receiver
    let receiver = format_ident!("{}Receiver", trait_name);

    let function_match_arms = methods.iter().map(|method| {
        let name = method.name;
        quote! {
            stringify!(#name) => {
                let reply = self
                    .service
                    .#name(serde_json::from_str(&call.args).expect("rpc parse failed"))
                    .await;
                serde_json::to_string(&reply).expect("serialize failed")
            },
        }
    });

    let rpc_receiver = quote! {
        pub struct #receiver<T: #trait_name + Send + Sync + 'static> {
            network_state: std::sync::Arc<tokio::sync::Mutex<labrpc_network::NetworkState>>,
            service: std::sync::Arc<T>
        }

        impl<T: #trait_name + Send + Sync + 'static> Clone for #receiver<T> {
            #[inline]
            fn clone(&self) -> #receiver<T> {
                #receiver {
                    network_state: ::core::clone::Clone::clone(&self.network_state),
                    service: ::core::clone::Clone::clone(&self.service),
                }
            }
        }

        impl<T: #trait_name + Send + Sync + 'static> #receiver<T> {
            fn new(
                receiver: tokio::sync::mpsc::Receiver<#rpc_call_struct_name>,
                service: std::sync::Arc<T>,
                network_state: std::sync::Arc<tokio::sync::Mutex<labrpc_network::NetworkState>>
            ) -> Self {
                let server = Self { network_state, service };
                let server_cloned = server.clone();
                tokio::spawn(async move { server_cloned.handler(receiver).await });
                server
            }

            async fn handler(&self, mut receiver: tokio::sync::mpsc::Receiver<#rpc_call_struct_name>) {
                loop {
                    let call = match receiver.recv().await {
                        None => break,
                        Some(call) => call,
                    };

                    let network_state = self.network_state.lock().await.clone();
                    let simulated_result = network_state.simulate_network(()).await;

                    let simulated_result = if simulated_result.is_none() {
                        None
                    } else {
                        let call_ret_str = match call.func_name.as_str() {
                            #(#function_match_arms)*
                            _ => {
                                panic!("unknown call function: {}", call.func_name);
                            }
                        };

                        let network_state = self.network_state.lock().await.clone();

                        network_state.simulate_network(call_ret_str).await
                    };

                    call.sender
                        .send(simulated_result)
                        .await
                        .expect("rpc send failed");
                }
            }
        }
    };

    // implement peer
    let peer = format_ident!("{}Peer", trait_name);

    let rpc_peer = quote! {
        pub struct #peer;

        impl #peer {
            pub fn create<T: #trait_name + Send + Sync + 'static>(
                service: std::sync::Arc<T>,
            ) -> (
                #sender,
                #receiver<T>,
                std::sync::Arc<tokio::sync::Mutex<labrpc_network::NetworkState>>,
            ) {
                let (sender, receiver) = tokio::sync::mpsc::channel(100);

                let network_state = std::sync::Arc::new(tokio::sync::Mutex::new(labrpc_network::NetworkState::default()));
                let sender = #sender { sender };
                let receiver = #receiver::<T>::new(receiver, service, network_state.clone());

                (sender, receiver, network_state)
            }
        }
    };

    TokenStream::from(quote! {
        #import
        #trait_rewrite
        #rpc_sender
        #rpc_receiver
        #rpc_peer
    })
}
