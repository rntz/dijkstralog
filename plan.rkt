#lang racket

(define-syntax-rule (todo) (error "todo"))
(define (assert! test) (unless test (error "ASSERTION FAILURE")))

(define atom-pred car)
(define atom-args cdr)

(define var? symbol?)

;; A notion of subsequence that allows terms to be duplicated in xs. We assume
;; terms are not duplicated in ys but do not check this. We can use a term from
;; ys multiple times. eg:
;;
;; (subsequence '(a c) '(a b c))   ==> (0 2)
;; (subsequence '(b b) '(a b c))   ==> (1 1)
;; (subsequence '(b a) '(a b c))   ==> #f
;; (subsequence '(a b a) '(a b c)) ==> #f
;; (subsequence '(a b b) '(a b c)) ==> (0 1 1)
(define (subsequence xs ys #:by [equal equal?])
  (let loop ([acc '()] [i 0] [xs xs] [ys ys])
    (match* (xs ys)
      [('() _) (reverse acc)]
      [(_ '()) #f]
      [(xs ys)
       (if (equal (car xs) (car ys))
           (loop (cons i acc) i       (cdr xs) ys)
           (loop acc          (+ 1 i) xs       (cdr ys)))])))

;; (compile var-order query) --> (values query-plan decls)
;; decls: a list of indices that we need for this query
(define (compile var-order query)
  (define decls '())
  ;; NOTE/TODO: we could get rid of constants and duplicated variables up front
  ;; (as operations on trie iterators), instead of pushing that work through.
  ;; That would avoid the need to handle it separately in each of
  ;; sorting-and-projection and conjunctive-querying.
  (define query-ordered
    (for/list ([atom query])
      (define atom-vars (filter var? (atom-args atom)))
      (if (subsequence atom-vars var-order) atom
          ;; Otherwise, need to re-sort!
          (let* ([new-pred (gensym (atom-pred atom))]
                 [new-vars (filter (lambda (x) (member x atom-vars)) var-order)]
                 [new-atom `(,new-pred ,@new-vars)]
                 ;; TODO: re-use indices via hash-consing re-sortings
                 [sort-decl `(sort ,new-atom ,atom)])
            (set! decls (cons sort-decl decls))
            new-atom))))
  ;; tracks our progress through each iterator in the query
  (define iter-states
    (for/list ([atom query-ordered])
      (mcons (atom-pred atom) (atom-args atom))))
  (define query-plan
   (for/list ([var var-order])
     ;; Find the relations which touch this variable and inner join them.
     `(,var <-
            join
            ,@(for/list ([iter iter-states]
                         #:when (member var (mcdr iter)))
                ;; TODO: do any constant (or repeated var) lookups necessary to
                ;; expose the variable.
                (assert! (equal? var (car (mcdr iter))))
                (set-mcdr! iter (cdr (mcdr iter)))
                (mcar iter) ;; TODO WRONG
                ))
     ))
  (values query-plan decls))

(module+ tests
  (compile '(a b c) '((R a b) (S b c) (T c 17 a)))
  (compile '(a b c) '((R a b) (S b c) (T a 17 c))) ;; FAILING ASSERT
  )
